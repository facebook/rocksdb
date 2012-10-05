# How To Release

Since levedbjni has to be build against multiple platforms, the standard maven release plugin will not work to do the release.

Once you ready to do the release, create a branch for the release using:

    git co -b ${version}.x

Update the version number in the poms using:

    mvn -P all org.codehaus.mojo:versions-maven-plugin:1.2:set org.codehaus.mojo:versions-maven-plugin:1.2:commit -DnewVersion="${version}" 
    git commit -am "Preping for a the ${version} release"
    git tag "leveldbjni-${version}"
    git push origin "leveldbjni-${version}"

Now release the non-platform specific artifacts using:

    mvn clean deploy -P release -P download

Then for each platform, shell into the box check out the "leveldbjni-${version}" tag and then:

    cd $platform
    mvn clean deploy -Dleveldb=`cd ../../leveldb; pwd` -Dsnappy=`cd ../../snappy-1.0.3; pwd` -P release -P download

Finally release the `leveldbjni-all` which uber jars all the previously released artifacts.

    cd leveldbjni-all
    mvn clean deploy -P release -P download

Congrats your done.  Make sure your releasing the artifacts in Nexus after each step. 