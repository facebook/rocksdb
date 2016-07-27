# Derived from https://github.com/IvanSafonov/cmake-deb-packaging
# Making target for componet
function(make_deb_target COMPONENT)

    if( COMPONENT )
        set(TMP_COMPONENT ${COMPONENT})
        set(COMPONENT "_${COMPONENT}")

        # Install target for component
        add_custom_target(install${COMPONENT}
            COMMAND
             "${CMAKE_COMMAND}" -DCMAKE_INSTALL_COMPONENT=${TMP_COMPONENT}
             -P "${CMAKE_BINARY_DIR}/cmake_install.cmake"
        )
    endif()

    set(TMP_PACKAGE_DIR "${CMAKE_BINARY_DIR}/cmake_deb_package${COMPONENT}")
    set(TMP_PACKAGE_DEB_DIR "${TMP_PACKAGE_DIR}/DEBIAN")

    # Check required package fields
    if( NOT DEB_PACKAGE${COMPONENT}_NAME )
        message(FATAL_ERROR "Not set package name 'DEB_PACKAGE${COMPONENT}_NAME'")
    endif()
    
    if( NOT DEB_PACKAGE${COMPONENT}_VERSION )
        set(DEB_PACKAGE${COMPONENT}_VERSION ${CPACK_PACKAGE_VERSION})
    endif()
    
    if( NOT DEB_PACKAGE${COMPONENT}_DESRCIPTION )
        message(FATAL_ERROR "Not set package description 'DEB_PACKAGE${COMPONENT}_DESRCIPTION'")
    endif()

    if( NOT DEB_PACKAGE${COMPONENT}_MAINTAINER )
        set(DEB_PACKAGE${COMPONENT}_MAINTAINER ${CPACK_PACKAGE_CONTACT})
    endif()

    # Package Architecture
    if( NOT DEB_PACKAGE${COMPONENT}_ARCH )
        execute_process(COMMAND
            dpkg-architecture -qDEB_BUILD_ARCH
            OUTPUT_VARIABLE CURRENT_ARCH
        )
        string(STRIP "${CURRENT_ARCH}" CURRENT_ARCH)
        set(DEB_PACKAGE${COMPONENT}_ARCH "${CURRENT_ARCH}")
    endif()

    # Create control file
    set(GEN_CONTROL_LIST
        "Package: ${DEB_PACKAGE${COMPONENT}_NAME}"
        "Version: ${DEB_PACKAGE${COMPONENT}_VERSION}"
        "Architecture: ${DEB_PACKAGE${COMPONENT}_ARCH}"
        "Maintainer: ${DEB_PACKAGE${COMPONENT}_MAINTAINER}"
        "Description: ${DEB_PACKAGE${COMPONENT}_DESRCIPTION}"
    )

    # Adding field to control file
    function(add_to_control FIELD_NAME FIELD)
        if( FIELD )
            string(REPLACE ";" ", " TMP_FIELD "${FIELD}")
            set(GEN_CONTROL_LIST
                ${GEN_CONTROL_LIST}
                "${FIELD_NAME}: ${TMP_FIELD}"
                PARENT_SCOPE
            )
        endif()
    endfunction()
    
    add_to_control("Section" "${DEB_PACKAGE${COMPONENT}_SECTION}")
    add_to_control("Origin" "${DEB_PACKAGE${COMPONENT}_ORIGIN}")
    add_to_control("Bugs" "${DEB_PACKAGE${COMPONENT}_BUGS}")
    add_to_control("Homepage" "${DEB_PACKAGE${COMPONENT}_HOMEPAGE}")
    add_to_control("Depends" "${DEB_PACKAGE${COMPONENT}_DEPENDS}")
    add_to_control("Pre-Depends" "${DEB_PACKAGE${COMPONENT}_PREDEPENDS}")
    add_to_control("Recommends" "${DEB_PACKAGE${COMPONENT}_RECOMMENDS}")
    add_to_control("Suggests" "${DEB_PACKAGE${COMPONENT}_SUGGESTS}")
    add_to_control("Breaks" "${DEB_PACKAGE${COMPONENT}_BREAKS}")
    add_to_control("Conflicts" "${DEB_PACKAGE${COMPONENT}_CONFLICTS}")
    add_to_control("Replaces" "${DEB_PACKAGE${COMPONENT}_REPLACES}")
    add_to_control("Provides" "${DEB_PACKAGE${COMPONENT}_PROVIDES}")

    string(REPLACE ";" "\\n" GEN_CONTROL_STR "${GEN_CONTROL_LIST}")

    set(PACKAGE_FILENAME "${CMAKE_BINARY_DIR}/${DEB_PACKAGE${COMPONENT}_NAME}_${DEB_PACKAGE${COMPONENT}_VERSION}_${DEB_PACKAGE${COMPONENT}_ARCH}.deb")

    # Build package target for component
    add_custom_target(deb_package${COMPONENT}
        COMMAND rm -rf ${TMP_PACKAGE_DIR}
        COMMAND mkdir -p ${TMP_PACKAGE_DEB_DIR}
        COMMAND echo ${GEN_CONTROL_STR} > ${TMP_PACKAGE_DEB_DIR}/control
        COMMAND test -z "${DEB_PACKAGE${COMPONENT}_CONTROL_FILES}" || cp ${DEB_PACKAGE${COMPONENT}_CONTROL_FILES} -t ${TMP_PACKAGE_DEB_DIR}
        COMMAND make DESTDIR=${TMP_PACKAGE_DIR} install${COMPONENT}
        COMMAND echo -n "Installed-Size: " >> ${TMP_PACKAGE_DEB_DIR}/control
        COMMAND du -sx --exclude DEBIAN ${TMP_PACKAGE_DIR} | grep -o -E ^[0-9]+ >> ${TMP_PACKAGE_DEB_DIR}/control
        COMMAND dpkg-deb --build ${TMP_PACKAGE_DIR} ${PACKAGE_FILENAME}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        VERBATIM
    )
endfunction()

# if build multiple packages
if(DEB_PACKAGE_COMPONENTS)
    foreach(COMP ${DEB_PACKAGE_COMPONENTS})
        make_deb_target(${COMP})
        set(TMP_DEPENDS ${TMP_DEPENDS} "deb_package_${COMP}")
    endforeach()

    # common target for build all packages
    add_custom_target(deb_package
        DEPENDS ${TMP_DEPENDS}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )
else()
    make_deb_target("")
endif()
