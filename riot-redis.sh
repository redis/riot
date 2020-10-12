#!/bin/sh -e

gradle -q --console plain installDist

./riot-redis/build/install/riot-redis/bin/riot-redis "$@"
