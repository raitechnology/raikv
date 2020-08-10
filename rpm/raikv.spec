Name:		raikv
Version:	999.999
Vendor:	        Rai Technology, Inc
Release:	99999%{?dist}
Summary:	Rai key value cache

License:	ASL 2.0
URL:		https://github.com/raitechnology/%{name}
Source0:	%{name}-%{version}-99999.tar.gz
BuildRoot:	${_tmppath}
Prefix:	        /usr
BuildRequires:  gcc-c++
BuildRequires:  chrpath
BuildRequires:  systemd
Requires(pre): shadow-utils
Requires(post): /sbin/ldconfig
Requires(postun): /sbin/ldconfig, /usr/sbin/userdel, /usr/sbin/groupdel

%description
A shared memory, concurrent access, key value caching system.

%prep
%setup -q


%define _unpackaged_files_terminate_build 0
%define _missing_doc_files_terminate_build 0
%define _missing_build_ids_terminate_build 0
%define _include_gdb_index 1

%build
make build_dir=./usr %{?_smp_mflags} dist_bins
cp -a ./include ./usr/include
mkdir -p ./usr/share/doc/%{name}
cp -a ./README.md graph ./usr/share/doc/%{name}/

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}

# in builddir
cp -a * %{buildroot}

install -p -D -m 644 ./config/limit.conf %{buildroot}%{_sysconfdir}/systemd/system/%{name}.service.d/limit.conf
sed -e 's:/usr/bin:%{_bindir}:;s:/var:%{_localstatedir}:;s:/etc:%{_sysconfdir}:;s:/usr/libexec:%{_libexecdir}:' \
     ./config/%{name}.service > tmp_file
install -p -D -m 644 tmp_file %{buildroot}%{_unitdir}/%{name}.service

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_prefix}/lib64/*
%{_includedir}/*
%{_prefix}/share/doc/*
%config(noreplace) %{_sysconfdir}/systemd/system/%{name}.service.d/limit.conf
%config(noreplace) %{_unitdir}/%{name}.service

%pre
getent group %{name} &> /dev/null || \
groupadd -r %{name} &> /dev/null
getent passwd %{name} &> /dev/null || \
useradd -r -g %{name} -d %{_sharedstatedir}/%{name} -s /sbin/nologin -c "Rai KV" %{name}
exit 0

%post
echo "%{_prefix}/lib64" > /etc/ld.so.conf.d/%{name}.conf
/sbin/ldconfig
%systemd_post %{name}.service

%preun
%systemd_preun %{name}.service

%postun
if [ $1 -eq 0 ] ; then
  rm -f /etc/ld.so.conf.d/%{name}.conf
fi
/sbin/ldconfig
/usr/sbin/userdel %{name}

%changelog
* __DATE__ <support@raitechnology.com>
- Hello world
