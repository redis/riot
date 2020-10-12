#!/bin/sh -e

gradle -q --console plain installDist

./riot-db/build/install/riot-db/bin/riot-db "$@"
