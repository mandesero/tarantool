%global scl 16

%{?scl:%global _scl_prefix /opt/tarantool}
%{?scl:%scl_package}

%{?scl:%global scl_name tarantool-%scl}

Summary: Package that installs %scl Software Collection.
Name: %scl_name
Version: 1.0
Release: 1%{?dist}
Buildarch: noarch
License: BSD
Group: Applications/File
Requires: %{scl_prefix}tarantool
Requires: %{scl_prefix}tarantool-dev
Requires: %{scl_prefix}tarantool-sql-module
Requires: %{scl_prefix}tarantool-pg-module
Requires: %{scl_prefix}tarantool-mysql-module
%{?scl:BuildRequires: scl-utils-build}
%{?scl:BuildRequires: iso-codes}

%description
This is the main package for %scl_name Software Collection.

%package build
Summary: Package shipping basic build configuration
Group: Applications/File

%description build
Package shipping essential configuration macros to build %scl Software Collection.

%package runtime
Summary: Package that handles %scl_name Software Collection.
Group: Applications/File
Requires: scl-utils

%description runtime
Package shipping essential configuration macros to build %scl Software Collection.

%prep
%setup -c -T

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/%{_scl_scripts}/root
cat > %{buildroot}/%{_scl_scripts}/enable <<\EOF
export PATH=%{_bindir}${PATH+:${PATH}}
export MANPATH=%{_mandir}:$MANPATH
export INFOPATH=%{_infodir}${INFOPATH:+:${INFOPATH}}
export LD_LIBRARY_PATH=%{_libdir}:\$LD_LIBRARY_PATH
EOF
%scl_install

%files

%files build
%{_root_sysconfdir}/rpm/macros.%{scl}-config

%files runtime
%scl_files

%changelog
* Thu Jun 12 2014 Blikh Eugine <bigbes@gmail.com> 1.0-2
- Port to 1.6 from 1.5
* Fri Jun 06 2014 Blikh Eugine <bigbes@gmail.com> 1.0-1
- Initial release
- Initscripts are broken.
