#!/bin/sh -e

gradle -q --console plain installDist

./riot-file/build/install/riot-file/bin/riot-file "$@"
