#!/bin/bash

# Script to run SwiftLint on the project

echo "Running SwiftLint..."

# Check if SwiftLint is installed
if ! command -v swiftlint &> /dev/null; then
    echo "SwiftLint is not installed."
    echo "https://github.com/realm/SwiftLint"
    exit 1
fi

# Run SwiftLint
swiftlint --config .swiftlint.yml

# Check the exit code
if [ $? -eq 0 ]; then
    echo "✅ SwiftLint passed with no issues"
else
    echo "❌ SwiftLint found issues"
    exit 1
fi