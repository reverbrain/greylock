Summary:	Greylock is an embedded search engine
Name:		greylock
Version:	1.0.0
Release:	1%{?dist}.1

License:	GPLv3
Group:		System Environment/Libraries
URL:		http://reverbrain.com/
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)


BuildRequires:	ribosome-devel
BuildRequires:	libswarm3-devel, libthevoid3-devel
BuildRequires:	boost-devel, boost-system, boost-program-options, boost-filesystem
BuildRequires:	msgpack-devel, lz4-devel
BuildRequires:	cmake >= 2.6

%description
Greylock is an embedded search engine which is aimed at index size and performace.
Index of 200k livejournal.com entries (200Mb of uncompressed data) takes about 450Mb,
index includes: full-text and per-author search indexes, original content, stemmed and original content.

%package devel
Summary: Development files for %{name}
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}


%description devel
Greylock is an embedded search engine which is aimed at index size and performace.

This package contains libraries, header files and developer documentation
needed for developing software which uses greylock utils.

%prep
%setup -q

%build
export LDFLAGS="-Wl,-z,defs"
export DESTDIR="%{buildroot}"
%{cmake} .
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make install DESTDIR="%{buildroot}"

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/greylock_server
%{_bindir}/greylock_meta
%{_libdir}/libgreylock.so.*
%doc conf/


%files devel
%defattr(-,root,root,-)
%{_includedir}/*
%{_datadir}/greylock/cmake/*
%{_libdir}/libgreylock.so

%changelog
