import XCTest

import FileBackedKeyedPersistenceTests

var tests = [XCTestCaseEntry]()
tests += FileBackedKeyedPersistenceTests.allTests()
XCTMain(tests)
