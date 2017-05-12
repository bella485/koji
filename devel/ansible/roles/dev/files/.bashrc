# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

shopt -s expand_aliases

function ktest {
    find /home/vagrant/koji -name "*.pyc" -delete;
    pushd /home/vagrant/koji && make test; popd
}

export PYTHONWARNINGS="once"

cd koji
