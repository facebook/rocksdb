%define _tmppath %{_topdir}/BUILDROOT
%define is_etc  %(test -z $etc && echo 0 || echo 1)
%define is_not_etc  %(test -z $etc && echo 1 || echo 0)
%define PACKAGE_HOME %{_datadir}/uc
%if %{is_etc}
%define _sysconfdir %{_prefix}/etc
%endif

Name:        rocksdb 
Version:     4.8.0
Release:     1%{?dist}

Summary:     A Persistent Key-Value Store for Flash and RAM Storage
License:     GPLv3
Group:       Development/Libraries
Url:         http://github.com/facebook/rocksdb
Source0:     %{name}-%{version}.tar.gz
Patch0:      0001-fixed-rocksdb-for-fedora23.patch
%if %{is_etc}
Prefix:      %_prefix
%endif
BuildRoot:   %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id} -u -n)
Vendor:      Cnangel <cnangel@gmail.com>
Packager:    Cnangel <cnangel@gmail.com>
BuildRequires: gflags-devel 
BuildRequires: gtest-devel 

%description
%{name} is is a library that forms the core building block for a fast
key value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it specially suitable for storing multiple terabytes of data in a
single database.

%package devel
Summary: Header files, libraries for %{name}.
Group: %{group}
Requires: %{name} = %{version}-%{release} 

%description devel
This package contains the header files and static libraries for %{name}.
If you like to develop programs using %{name}, you will need to install %{name}-devel.

%prep
%setup -q
%patch0 -p1

%build
export PKG_CONFIG_PATH=mypkgconfig
sh ./autogen.sh
%configure
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
%makeinstall
install -m 0755 -d $RPM_BUILD_ROOT%{_includedir}/%{name}/utilities
install -pm 0644 include/%{name}/utilities/*.h $RPM_BUILD_ROOT%{_includedir}/%{name}/utilities
install -pm 0644 include/%{name}/*.h $RPM_BUILD_ROOT%{_includedir}/%{name}

%clean
rm -rf $RPM_BUILD_ROOT

%post
%if %{is_not_etc}
/sbin/ldconfig
%endif

%postun
%if %{is_not_etc}
/sbin/ldconfig
%endif

%files
%defattr(-,root,root,-)
%{_libdir}/*.so*
%attr(755,root,root) %{_bindir}/*

%files devel
%defattr(-,root,root,-)
%{_includedir}/*
%{_libdir}/*.a
%{_libdir}/*.so
%{_libdir}/pkgconfig/*.pc
%exclude %{_libdir}/*.la

%changelog
* Tue May 10 2016 Cnangel <cnangel@gmail.com> 4.8.0-1
- upgrade version 4.8.0
* Mon May 09 2016 Cnangel <cnangel@gmail.com> 4.5.1-1
- create the frist spec file.


# -fin-
