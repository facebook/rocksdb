## Cross-building

To build RocksDB as a single self contained cross-platform JAR. The cross-platform jar can be usd on any 64-bit OSX system, 32-bit Linux system, or 64-bit Linux system.

Building a cross-platform JAR requires:

 * [Vagrant](https://www.vagrantup.com/)
 * [Virtualbox](https://www.virtualbox.org/)
 * A Mac OSX machine

Once you have these items, run this make command from RocksDB's root source directory:

    make jclean clean rocksdbjavastaticrelease

This command will build RocksDB natively on OSX, and will then spin up two Vagrant Virtualbox Ubuntu images to build RocksDB for both 32-bit and 64-bit Linux. 

You can find all native binaries and JARs in the java directory upon completion:

    librocksdbjni-linux32.so
    librocksdbjni-linux64.so
    librocksdbjni-osx.jnilib
    rocksdbjni-all.jar
    rocksdbjni-linux32.jar
    rocksdbjni-linux64.jar
    rocksdbjni-osx.jar

## Maven publication

TODO
