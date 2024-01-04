import XCTest
@testable import FileBackedKeyedPersistence

final class FileBackedKeyedPersistenceTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        //XCTAssertEqual(FileBackedKeyedPersistence().text, "Hello, World!")
        
        let directory = KeyedPersistenceDirectory(inTemporaryDirectoryWithName: "com.meteor.FileBackedKeyedPersistence.test")
        let item: KeyedPersistenceDirectory.Item<String> = directory.makeItem(key: UUID().uuidString)
        XCTAssert(try! item.get() == nil)
        try! item.set("hello")
        XCTAssert(item.value == "hello")
        item.clearCache()
        XCTAssert(item.value == "hello")
        try! directory.clear()
        XCTAssert(directory.currentDiskUsage == 0)
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
