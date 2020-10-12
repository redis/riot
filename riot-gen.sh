#!/bin/sh -e

gradle -q --console plain installDist

./riot-gen/build/install/riot-gen/bin/riot-gen "$@"
