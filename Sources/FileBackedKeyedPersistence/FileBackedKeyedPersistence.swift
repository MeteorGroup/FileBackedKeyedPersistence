import Foundation
import CommonCrypto
import ConcurrencyUtilities

public protocol PersistenceItemSerializer {
    associatedtype Item
    func encode(_ value: Item) throws -> Data
    func decode(_ data: Data) throws -> Item
}

@propertyWrapper
public struct FileBacked<T> {
    public let defaultValue: T
    
    public var wrappedValue: T {
        set {
            self.item.value = newValue
        }
        get {
            return item.value ?? self.defaultValue
        }
    }
    
    private var item: KeyedPersistenceDirectory.Item<T>
    
    public init(item: KeyedPersistenceDirectory.Item<T>, `default`: T) {
        self.item = item
        self.defaultValue = `default`
    }
}

extension FileBacked where T: ExpressibleByNilLiteral {
    public init(item: KeyedPersistenceDirectory.Item<T>, `default`: T) {
        fatalError("You may want to use FileBackedOptional<T>")
    }
}

@propertyWrapper
public struct FileBackedOptional<T> {
    public var wrappedValue: T? {
        set {
            self.item.value = newValue
        }
        get {
            return item.value
        }
    }
    private var item: KeyedPersistenceDirectory.Item<T>
    public init(item: KeyedPersistenceDirectory.Item<T>) {
        self.item = item
    }
}

public struct KeyedPersistenceDirectory {
    
    public struct AnySerializer<T>: PersistenceItemSerializer {
        public typealias Item = T
        
        public func encode(_ value: T) throws -> Data {
            return try self.encoder(value)
        }
        
        public func decode(_ data: Data) throws -> T {
            return try self.decoder(data)
        }
        
        private let encoder: (T) throws -> Data
        private let decoder: (Data) throws -> T
        
        public init<S>(_ serializer: S) where S: PersistenceItemSerializer, S.Item == T {
            encoder = serializer.encode
            decoder = serializer.decode
        }
    }
    
    public struct CodableSerializer<T>: PersistenceItemSerializer where T: Codable {
        public typealias Item = T
        
        private let encoder = PropertyListEncoder()
        private let decoder = PropertyListDecoder()
        
        private struct Wrapper: Codable {
            let root: T
            init(_ value: T) {
                self.root = value
            }
        }
        
        public func encode(_ value: T) throws -> Data {
            return try encoder.encode(Wrapper(value))
        }
        
        public func decode(_ data: Data) throws -> T {
            return (try decoder.decode(Wrapper.self, from: data)).root
        }
    }
    
    public struct PassthroughSerializer: PersistenceItemSerializer {
        public typealias Item = Data
        
        public func encode(_ value: Data) throws -> Data {
            return value
        }
        
        public func decode(_ data: Data) throws -> Data {
            return data
        }
    }
    
    public struct KeyedArchivingSerializer<T>: PersistenceItemSerializer where T: NSObject & NSSecureCoding {
        
        public enum Error: Swift.Error {
            case nilValue
        }
        
        public typealias Item = T
        
        public func encode(_ value: T) throws -> Data {
            return try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
        }
        
        public func decode(_ data: Data) throws -> T {
            if let value = try NSKeyedUnarchiver.unarchivedObject(ofClass: T.self, from: data) {
                return value
            }
            throw Error.nilValue
        }
    }
    
    public struct ItemOptions: OptionSet {
        public let rawValue: Int
        public init(rawValue: Int) {
            self.rawValue = rawValue
        }
        public static let asyncWriteDisk = ItemOptions(rawValue: 1 << 0)
    }
    
    public struct Item<T> {
        private let directory: KeyedPersistenceDirectory
        private let key: String
        private let serializer: AnySerializer<T>
        
        public let url: URL
        
        private let options: ItemOptions
        
        public init<S>(directory: KeyedPersistenceDirectory, key: String, serializer: S, options: ItemOptions = []) where S: PersistenceItemSerializer, S.Item == T {
            self.directory = directory
            self.key = key
            self.serializer = AnySerializer<T>(serializer)
            self.url = directory.fileURL(for: key)
            self.options = options
        }
        
        public func set(_ value: T?) throws {
            if let value = value {
                if self.options.contains(.asyncWriteDisk) {
                    self.directory.ioQueue.async(execute: DispatchWorkItem(qos: .default, flags: .barrier, block: {
                        do {
                            try self.directory.createIfNeeded()
                            try self.serializer.encode(value).write(to: self.url, options: .atomic)
                        } catch {
                            assertionFailure(error.localizedDescription)
                        }
                    }))
                } else {
                    try self.directory.ioQueue.sync(flags: .barrier) {
                        try self.directory.createIfNeeded()
                        let data = try self.serializer.encode(value)
                        try data.write(to: self.url, options: .atomic)
                    }
                }
            } else {
                if self.options.contains(.asyncWriteDisk) {
                    self.directory.ioQueue.async(flags: .barrier) {
                        if self.directory.fileManager.fileExists(atPath: self.url.path) {
                            do {
                                try self.directory.fileManager.removeItem(at: self.url)
                            } catch {
                                assertionFailure(error.localizedDescription)
                            }
                        }
                    }
                } else {
                    try self.directory.ioQueue.sync(flags: .barrier) {
                        if self.directory.fileManager.fileExists(atPath: self.url.path) {
                            try self.directory.fileManager.removeItem(at: self.url)
                        }
                    }
                }
            }
            
            self.cache.value = value
            self.cache.isVaild = true
        }
        
        private class Cache {
            var value: T?
            var isVaild: Bool = false
        }
        
        private let cache = Cache()
        
        internal func clearCache() {
            self.cache.value = nil
            self.cache.isVaild = false
        }
        
        public func get() throws -> T?  {
            if self.cache.isVaild {
                return self.cache.value
            }
            let value: T? = try self.directory.ioQueue.sync {
                if self.directory.fileManager.fileExists(atPath: self.url.path) {
                    return try self.serializer.decode(Data(contentsOf: self.url))
                } else {
                    return nil
                }
            }
            self.cache.value = value
            self.cache.isVaild = true
            return value
        }
        
        public var value: T? {
            get {
                do {
                    return try self.get()
                } catch {
                    assertionFailure(error.localizedDescription)
                    return nil
                }
            }
            set {
                do {
                    try self.set(newValue)
                } catch {
                    assertionFailure(error.localizedDescription)
                }
            }
        }
    }
    
    @inlinable
    internal static func md5Hash(for string: String) -> String {
        let length = Int(CC_MD5_DIGEST_LENGTH)
        let messageData = string.data(using:.utf8)!
        var digestData = Data(count: length)
        _ = digestData.withUnsafeMutableBytes { digestBytes -> UInt8 in
            messageData.withUnsafeBytes { messageBytes -> UInt8 in
                if let messageBytesBaseAddress = messageBytes.baseAddress, let digestBytesBlindMemory = digestBytes.bindMemory(to: UInt8.self).baseAddress {
                    let messageLength = CC_LONG(messageData.count)
                    CC_MD5(messageBytesBaseAddress, messageLength, digestBytesBlindMemory)
                }
                return 0
            }
        }
        return digestData.map({ String(format: "%02hhx", $0) }).joined()
    }
    
    private struct WeakObjectWrapper<T> where T: AnyObject {
        weak var wrapped: T?
    }
    
    private static var ioQueues: [URL: WeakObjectWrapper<DispatchQueue>] = [:]
    private static var ioQueuesLock = UnfairLock()
    
    public let url: URL
    
    private let fileManager: FileManager
    
    private let ioQueue: DispatchQueue
    
    private init(url: URL) {
        KeyedPersistenceDirectory.ioQueuesLock.lock()
        defer {
            KeyedPersistenceDirectory.ioQueuesLock.unlock()
        }
        self.url = url
        self.fileManager = FileManager()
        if let exisitingQueue = KeyedPersistenceDirectory.ioQueues[url]?.wrapped {
            self.ioQueue = exisitingQueue
        } else {
            self.ioQueue = DispatchQueue(label: "com.meteor.KeyValuePersistenceDirectory.ioQueue", qos: .default, attributes: .concurrent, autoreleaseFrequency: .workItem, target: nil)
            KeyedPersistenceDirectory.ioQueues[url] = WeakObjectWrapper(wrapped: self.ioQueue)
        }
    }
    
    public init(in directory: URL, name: String) {
        precondition(directory.isFileURL)
        let url = directory.appendingPathComponent("com.meteor.KeyValuePersistenceDirectories").appendingPathComponent(KeyedPersistenceDirectory.md5Hash(for: name))
        self.init(url: url)
    }
    
    public init(directory: FileManager.SearchPathDirectory, name: String) throws {
        self.init(in: try FileManager.default.url(for: directory, in: .userDomainMask, appropriateFor: nil, create: true), name: name)
    }
    
    public init(inTemporaryDirectoryWithName name: String) {
        self.init(in: FileManager.default.temporaryDirectory, name: name)
    }
    
    private func createIfNeeded() throws {
        var isDirectory: ObjCBool = false
        if self.fileManager.fileExists(atPath: self.url.path, isDirectory: &isDirectory) && isDirectory.boolValue {
            
        } else {
            try? self.fileManager.removeItem(at: self.url)
            #if os(iOS)
            try self.fileManager.createDirectory(at: self.url, withIntermediateDirectories: true, attributes: [FileAttributeKey.protectionKey: FileProtectionType.none])
            #else
            try self.fileManager.createDirectory(at: self.url, withIntermediateDirectories: true, attributes: nil)
            #endif
        }
    }
    
    public func clear() throws {
        try self.ioQueue.sync(flags: .barrier) {
            if self.fileManager.fileExists(atPath: self.url.path) {
                try self.fileManager.removeItem(at: self.url)
            }
        }
    }
    
    public var currentDiskUsage: Int {
        return self.ioQueue.sync(execute: {
            if let contents = try? fileManager.contentsOfDirectory(at: self.url, includingPropertiesForKeys: [.fileSizeKey], options: []) {
                return contents.reduce(0) { size, url in
                    size + ((try? url.resourceValues(forKeys: Set<URLResourceKey>([.fileSizeKey])).fileSize) ?? 0)
                }
            }
            return 0
        })
    }
    
    @inlinable
    public func fileURL(for key: String) -> URL {
        let pathExtension = self.url.appendingPathComponent(key).pathExtension
        return self.url.appendingPathComponent(KeyedPersistenceDirectory.md5Hash(for: key)).appendingPathExtension(pathExtension)
    }
    
    public func data(for key: String) throws -> Data? {
        let url = self.fileURL(for: key)
        return try self.ioQueue.sync {
            if self.fileManager.fileExists(atPath: url.path) {
                return try Data(contentsOf: url)
            } else {
                return nil
            }
        }
    }
    
    public func write(data: Data?, for key: String) throws {
        let url = self.fileURL(for: key)
        try self.ioQueue.sync(flags: .barrier) {
            if let data = data {
                try createIfNeeded()
                try data.write(to: url, options: .atomic)
            } else {
                if self.fileManager.fileExists(atPath: url.path) {
                    try self.fileManager.removeItem(at: url)
                }
            }
        }
    }
}

public extension KeyedPersistenceDirectory {
    func makeItem<T, S>(key: String, serializer: S, options: ItemOptions = []) -> Item<T> where S: PersistenceItemSerializer, S.Item == T {
        return Item(directory: self, key: key, serializer: serializer, options: options)
    }
    
    func makeItem<T>(key: String, options: ItemOptions = []) -> Item<T> where T: Codable {
        return Item(directory: self, key: key, serializer: CodableSerializer<T>(), options: options)
    }
    
    func makeItem(key: String, options: ItemOptions = []) -> Item<Data> {
        return Item<Data>(directory: self, key: key, serializer: PassthroughSerializer(), options: options)
    }
    
    func makeItem<T>(key: String, options: ItemOptions = []) -> Item<T> where T: NSObject, T: NSSecureCoding {
        return Item(directory: self, key: key, serializer: KeyedArchivingSerializer<T>(), options: options)
    }
}
