#!/bin/sh
set -e

EXITCODE=0

# rancher/k3s modified from
# https://github.com/moby/moby/blob/c831882/contrib/check-config.sh

# bits of this were adapted from lxc-checkconfig for moby
# see also https://github.com/lxc/lxc/blob/lxc-1.0.2/src/lxc/lxc-checkconfig.in

uname=$(uname -r)
possibleConfigs="
  /proc/config.gz
  /boot/config-${uname}
  /boot/config-${uname##*-}
  /usr/src/linux-${uname}/.config
  /usr/src/linux/.config
"
binDir=$(dirname "$0")
configFormat=gz
isError=0

if [ $# -gt 0 ]; then
  CONFIG="$1"
fi

if ! command -v zgrep >/dev/null 2>&1; then
  zgrep() {
    zcat "$2" | grep "$1"
  }
fi

dogrep() {
  if [ "$configFormat" = "gz" ]; then
    zgrep "$1" "$2"
  else
    grep "$1" "$2"
  fi
}

kernelVersion="$(uname -r)"
kernelMajor="${kernelVersion%%.*}"
kernelMinor="${kernelVersion#$kernelMajor.}"
kernelMinor="${kernelMinor%%.*}"

is_set() {
  dogrep "CONFIG_$1=[y|m]" "$CONFIG" > /dev/null
}
is_set_in_kernel() {
  dogrep "CONFIG_$1=y" "$CONFIG" > /dev/null
}
is_set_as_module() {
  dogrep "CONFIG_$1=m" "$CONFIG" > /dev/null
}

color() {
  codes=
  if [ "$1" = 'bold' ]; then
    codes=1
    shift
  fi
  if [ "$#" -gt 0 ]; then
    code=
    case "$1" in
      # see https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
      black) code=30 ;;
      red) code=31 ;;
      green) code=32 ;;
      yellow) code=33 ;;
      blue) code=34 ;;
      magenta) code=35 ;;
      cyan) code=36 ;;
      white) code=37 ;;
    esac
    if [ "$code" ]; then
      [ "$codes" ] && codes="${codes};"
      codes="${codes}${code}"
    fi
  fi
  printf '\033['"$codes"'m'
}
wrap_color() {
  text="$1"
  shift
  color "$@"
  echo -n "$text"
  color reset
  echo
}

wrap_good() {
  echo "$(wrap_color "$1" white): $(wrap_color "$2" green)"
}
wrap_bad() {
  echo "$(wrap_color "$1" bold): $(wrap_color "$2 (fail)" bold red)"
  EXITCODE=$(($EXITCODE+1))
}
wrap_warn() {
  echo "$(wrap_color "$1" bold): $(wrap_color "$2" bold yellow)"
}
warning() {
  wrap_color >&2 "$*" yellow
}

check_flag() {
  if is_set_in_kernel "$1"; then
    wrap_good "CONFIG_$1" 'enabled'
  elif is_set_as_module "$1"; then
    wrap_good "CONFIG_$1" 'enabled (as module)'
  else
    if [ $isError -eq 1 ]; then
      wrap_bad "CONFIG_$1" 'missing'
    else
      wrap_warn "CONFIG_$1" 'missing'
    fi
  fi
}

check_flags() {
  for flag in "$@"; do
    echo -n "- "; check_flag "$flag"
  done
}

check_command() {
  if command -v "$1" >/dev/null 2>&1; then
    wrap_good "$1 command" 'available'
  else
    wrap_bad "$1 command" 'missing'
  fi
}

check_device() {
  if [ -c "$1" ]; then
    wrap_good "$1" 'present'
  else
    wrap_bad "$1" 'missing'
  fi
}

check_distro_userns() {
  [ -s /etc/os-release ] || return 0
  source /etc/os-release 2>/dev/null || true
  if ( echo "${ID}" | grep -q -E '^(centos|rhel)$' ) && \
    ( echo "${VERSION_ID}" | grep -q -E '^7' ); then
    # this is a CentOS7 or RHEL7 system
    if ! grep -q "user_namespace.enable=1" /proc/cmdline; then
      # no user namespace support enabled
      wrap_bad "  (RHEL7/CentOS7" "User namespaces disabled; add 'user_namespace.enable=1' to boot command line)"
    fi
  fi
}

# ---

echo

{
  cd $binDir
  echo "Verifying binaries in $binDir:"

  if [ -s .sha256sums ]; then
    sumsTemp=$(mktemp)
    if sha256sum -c .sha256sums >$sumsTemp 2>&1; then
      wrap_good '- sha256sum' 'good'
    else
      wrap_bad '- sha256sum' 'does not match'
      cat $sumsTemp | sed 's/^/  ... /'
    fi
    rm -f $sumsTemp
  else
    wrap_warn '- sha256sum' 'sha256sums unavailable'
  fi

  linkFail=0
  if [ -s .links ]; then
    while read file link; do
      if [ "$(readlink $file)" != "$link" ]; then
        wrap_bad '- links' "$file should link to $link"
        linkFail=1
      fi
    done <.links
    if [ $linkFail -eq 0 ]; then
      wrap_good '- links' 'good'
    fi
  else
    wrap_warn '- links' 'link list unavailable'
  fi

  cd - >/dev/null
}

echo

{
  version_ge() {
    [ "$1" = "$2" ] || [ "$(printf '%s\n' "$@" | sort -V | head -n 1)" != "$1" ]
  }
  version_less() {
    [ "$(printf '%s\n' "$@" | sort -rV | head -n 1)" != "$1" ]
  }
  which_iptables() {
    (
      localIPtables=$(command -v iptables)
      PATH=$(printf "%s" "$(echo -n $PATH | tr ":" "\n" | grep -v -E "^$binDir$")" | tr "\n" ":")
      systemIPtables=$(command -v iptables)
      if [ -n "$systemIPtables" ]; then
        echo $systemIPtables
        return
      fi
      echo $localIPtables
    )
  }

  echo "System:"

  iptablesCmd=$(which_iptables)
  iptablesVersion=
  if [ "$iptablesCmd" ]; then
    iptablesInfo=$($iptablesCmd --version 2>/dev/null) || true
    iptablesVersion=$(echo $iptablesInfo | awk '{ print $2 }')
    label="$(dirname $iptablesCmd) $iptablesInfo"
  fi
  if echo "$iptablesVersion" | grep -v -q -E '^v[0-9]'; then
    [ "$iptablesCmd" ] || iptablesCmd="unknown iptables"
    wrap_warn "- $iptablesCmd" "unknown version: $iptablesInfo"
  elif version_ge $iptablesVersion v1.8.0; then
    iptablesMode=$(echo $iptablesInfo | awk '{ print $3 }')
    if [ "$iptablesMode" != "(legacy)" ] && version_less $iptablesVersion v1.8.4; then 
      wrap_bad "- $label" 'should be older than v1.8.0, newer than v1.8.3, or in legacy mode'
    else
      wrap_good "- $label" 'ok'
    fi
  else
    wrap_good "- $label" 'older than v1.8'
  fi

  totalSwap=$(free | grep -i '^swap:' | awk '{ print $2 }')
  if [ "$totalSwap" != "0" ]; then
    wrap_warn '- swap' 'should be disabled'
  else
    wrap_good '- swap' 'disabled'
  fi

  if ip route | grep -v cni0 | grep -q -E '^10\.(42|43)\.'; then
    wrap_warn '- routes' 'default CIDRs 10.42.0.0/16 or 10.43.0.0/16 already routed'
  else
    wrap_good '- routes' 'ok'
  fi
}

echo

{
  check_limit_over()
  {
    if [ "$(cat "$1")" -le "$2" ]; then
      wrap_bad "- $1" "$(cat "$1")"
      wrap_color "    This should be set to at least $2, for example set: sysctl -w kernel/keys/root_maxkeys=1000000" bold black
    else
      wrap_good "- $1" "$(cat "$1")"
    fi
  }

  echo 'Limits:'
  check_limit_over /proc/sys/kernel/keys/root_maxkeys 10000
}

echo

# ---

SUDO=
[ $(id -u) -ne 0 ] && SUDO=sudo
lsmod | grep -q configs || $SUDO modprobe configs || true

if [ -z "$CONFIG" ]; then
  for tryConfig in ${possibleConfigs}; do
    if [ -e "$tryConfig" ]; then
      CONFIG="$tryConfig"
      break
    fi
  done
fi
if [ ! -e "$CONFIG" ]; then
  case "$CONFIG" in
    -*)
      warning "error: argument $CONFIG"
      ;;
    *)
      warning "error: cannot find kernel config $CONFIG"
      ;;
  esac
  warning "  try running this script again, specifying the kernel config:"
  warning "  set CONFIG=/path/to/kernel/.config or add argument /path/to/kernel/.config"
  exit 1
fi

wrap_color "info: reading kernel config from $CONFIG ..." cyan
zcat $CONFIG >/dev/null 2>&1 || configFormat=

echo

echo 'Generally Necessary:'

cgroupV2FsType='63677270'
cgroupFsType="$(stat --file-system --format=%t /sys/fs/cgroup 2>/dev/null || :)"
cgroupHybridFsType="$(stat --file-system --format=%t /sys/fs/cgroup/unified 2>/dev/null || :)"

echo -n '- '
case "${cgroupFsType}:${cgroupHybridFsType}" in
  '':*)
    cgroupVariant='Nonexistent'
    ;;
  ${cgroupV2FsType}:*)
    cgroupVariant='V2'
    ;;
  *:${cgroupV2FsType})
    cgroupVariant='Hybrid'
    ;;
  *)
    cgroupVariant='V1'
    ;;
esac

case "${cgroupVariant}" in
  Nonexistent)
    wrap_bad 'cgroup hierarchy' "cgroups ${cgroupVariant}"
    ;;
  *)
    case "${cgroupVariant}" in
      V2)
        cgroupMatch='cpu|cpuset|memory'
        cgroupMatchNum=3
        cgroupFile='/sys/fs/cgroup/cgroup.controllers'
        ;;
      *)
        cgroupMatch='cpuset|memory'
        cgroupMatchNum=2
        cgroupFile='/proc/self/cgroup'
        ;;
    esac
    if [ "$(tr -s ' ' '\n' <"${cgroupFile}" 2>/dev/null | grep -Ec "^(${cgroupMatch})\$")" -eq ${cgroupMatchNum} ]; then
      cgroupStatus='good'
    else
      cgroupStatus='bad'
    fi
    wrap_${cgroupStatus} 'cgroup hierarchy' "cgroups ${cgroupVariant} mounted, ${cgroupMatch} controllers status: ${cgroupStatus}"
    if [ "x${cgroupStatus}" = 'xbad' ]; then
      warning '    (for cgroups V1/Hybrid on non-Systemd init see https://github.com/tianon/cgroupfs-mount)'
    fi
    ;;
esac

if [ "$(cat /sys/module/apparmor/parameters/enabled 2>/dev/null)" = 'Y' ]; then
  echo -n '- '
  if command -v apparmor_parser &> /dev/null; then
    wrap_good 'apparmor' 'enabled and tools installed'
  else
    wrap_bad 'apparmor' 'enabled, but apparmor_parser missing'
    echo -n '    '
    if command -v apt-get &> /dev/null; then
      wrap_color '(use "apt-get install apparmor" to fix this)'
    elif command -v yum &> /dev/null; then
      wrap_color '(your best bet is "yum install apparmor-parser")'
    else
      wrap_color '(look for an "apparmor" package for your distribution)'
    fi
  fi
fi

flags="
  NAMESPACES NET_NS PID_NS IPC_NS UTS_NS
  CGROUPS CGROUP_CPUACCT CGROUP_DEVICE CGROUP_FREEZER CGROUP_SCHED CPUSETS MEMCG
  KEYS
  VETH BRIDGE BRIDGE_NETFILTER
  IP_NF_FILTER IP_NF_TARGET_MASQUERADE
  NETFILTER_XT_MATCH_ADDRTYPE NETFILTER_XT_MATCH_CONNTRACK NETFILTER_XT_MATCH_IPVS
  IP_NF_NAT NF_NAT
  POSIX_MQUEUE
"
isError=1 check_flags $flags && isError=0

if [ "$kernelMajor" -lt 4 ] || ( [ "$kernelMajor" -eq 4 ] && [ "$kernelMinor" -lt 8 ] ); then
  check_flags DEVPTS_MULTIPLE_INSTANCES
fi

echo

echo 'Optional Features:'
{
  check_flags USER_NS
  check_distro_userns
}
{
  check_flags SECCOMP
}
{
  check_flags CGROUP_PIDS
}
# {
#   check_flags MEMCG_SWAP MEMCG_SWAP_ENABLED
#   if [ -e /sys/fs/cgroup/memory/memory.memsw.limit_in_bytes ]; then
#     echo "    $(wrap_color '(cgroup swap accounting is currently enabled)' bold black)"
#   elif is_set MEMCG_SWAP && ! is_set MEMCG_SWAP_ENABLED; then
#     echo "    $(wrap_color '(cgroup swap accounting is currently not enabled, you can enable it by setting boot option "swapaccount=1")' bold black)"
#   fi
# }
# {
#   if is_set LEGACY_VSYSCALL_NATIVE; then
#     echo -n "- "; wrap_bad "CONFIG_LEGACY_VSYSCALL_NATIVE" 'enabled'
#     echo "    $(wrap_color '(dangerous, provides an ASLR-bypassing target with usable ROP gadgets.)' bold black)"
#   elif is_set LEGACY_VSYSCALL_EMULATE; then
#     echo -n "- "; wrap_good "CONFIG_LEGACY_VSYSCALL_EMULATE" 'enabled'
#   elif is_set LEGACY_VSYSCALL_NONE; then
#     echo -n "- "; wrap_bad "CONFIG_LEGACY_VSYSCALL_NONE" 'enabled'
#     echo "    $(wrap_color '(containers using eglibc <= 2.13 will not work. Switch to' bold black)"
#     echo "    $(wrap_color ' "CONFIG_VSYSCALL_[NATIVE|EMULATE]" or use "vsyscall=[native|emulate]"' bold black)"
#     echo "    $(wrap_color ' on kernel command line. Note that this will disable ASLR for the,' bold black)"
#     echo "    $(wrap_color ' VDSO which may assist in exploiting security vulnerabilities.)' bold black)"
#   # else Older kernels (prior to 3dc33bd30f3e, released in v4.40-rc1) do
#   #      not have these LEGACY_VSYSCALL options and are effectively
#   #      LEGACY_VSYSCALL_EMULATE. Even older kernels are presumably
#   #      effectively LEGACY_VSYSCALL_NATIVE.
#   fi
# }

if [ "$kernelMajor" -lt 4 ] || ( [ "$kernelMajor" -eq 4 ] && [ "$kernelMinor" -le 5 ] ); then
  check_flags MEMCG_KMEM
fi

if [ "$kernelMajor" -lt 3 ] || ( [ "$kernelMajor" -eq 3 ] && [ "$kernelMinor" -le 18 ] ); then
  check_flags RESOURCE_COUNTERS
fi

if [ "$kernelMajor" -lt 3 ] || ( [ "$kernelMajor" -eq 3 ] && [ "$kernelMinor" -le 13 ] ); then
  netprio=NETPRIO_CGROUP
else
  netprio=CGROUP_NET_PRIO
fi

# IOSCHED_CFQ CFQ_GROUP_IOSCHED
flags="
  BLK_CGROUP BLK_DEV_THROTTLING
  CGROUP_PERF
  CGROUP_HUGETLB
  NET_CLS_CGROUP $netprio
  CFS_BANDWIDTH FAIR_GROUP_SCHED RT_GROUP_SCHED
  IP_NF_TARGET_REDIRECT
  IP_SET
  IP_VS
  IP_VS_NFCT
  IP_VS_PROTO_TCP
  IP_VS_PROTO_UDP
  IP_VS_RR
"
check_flags $flags

# if ! is_set EXT4_USE_FOR_EXT2; then
#   check_flags EXT3_FS EXT3_FS_XATTR EXT3_FS_POSIX_ACL EXT3_FS_SECURITY
#   if ! is_set EXT3_FS || ! is_set EXT3_FS_XATTR || ! is_set EXT3_FS_POSIX_ACL || ! is_set EXT3_FS_SECURITY; then
#     echo "    $(wrap_color '(enable these ext3 configs if you are using ext3 as backing filesystem)' bold black)"
#   fi
# fi

check_flags EXT4_FS EXT4_FS_POSIX_ACL EXT4_FS_SECURITY
if ! is_set EXT4_FS || ! is_set EXT4_FS_POSIX_ACL || ! is_set EXT4_FS_SECURITY; then
  if is_set EXT4_USE_FOR_EXT2; then
    echo "    $(wrap_color 'enable these ext4 configs if you are using ext3 or ext4 as backing filesystem' bold black)"
  else
    echo "    $(wrap_color 'enable these ext4 configs if you are using ext4 as backing filesystem' bold black)"
  fi
fi

echo '- Network Drivers:'
echo "  - \"$(wrap_color 'overlay' blue)\":"
check_flags VXLAN | sed 's/^/    /' # BRIDGE_VLAN_FILTERING
echo '      Optional (for encrypted networks):'
check_flags CRYPTO CRYPTO_AEAD CRYPTO_GCM CRYPTO_SEQIV CRYPTO_GHASH \
            XFRM XFRM_USER XFRM_ALGO INET_ESP INET_XFRM_MODE_TRANSPORT | sed 's/^/      /'
# echo "  - \"$(wrap_color 'ipvlan' blue)\":"
# check_flags IPVLAN | sed 's/^/    /'
# echo "  - \"$(wrap_color 'macvlan' blue)\":"
# check_flags MACVLAN DUMMY | sed 's/^/    /'
# echo "  - \"$(wrap_color 'ftp,tftp client in container' blue)\":"
# check_flags NF_NAT_FTP NF_CONNTRACK_FTP NF_NAT_TFTP NF_CONNTRACK_TFTP | sed 's/^/    /'

echo '- Storage Drivers:'
echo "  - \"$(wrap_color 'overlay' blue)\":"
check_flags OVERLAY_FS | sed 's/^/    /'

# ---

echo
if [ $EXITCODE -eq 0 ]; then
  wrap_good 'STATUS' 'pass'
else
  wrap_bad 'STATUS' $EXITCODE
fi

exit $EXITCODE
