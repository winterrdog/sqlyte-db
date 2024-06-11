#!/bin/bash

echo "+ compiling the source code..."

# check if gcc is installed
if ! [ -x "$(command -v gcc)" ]; then
    echo -e "- error: gcc is not installed.\n" >&2
    echo "+ falling back to clang compiler..."
    echo "+ using clang to compile the source code..."
    
    clang -Ofast -o sqlyte src/*.c
    
    exit 1
fi

echo "+ using gcc to compile the source code..."
gcc -Ofast -o sqlyte src/*.c