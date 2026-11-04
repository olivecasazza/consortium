# Running commands on many hosts over SSH

`claw` (clush), `pinch` (nodeset), and `molt` (clubak/dshbak) are the
everyday tools: name a set of hosts, run a command on all of them over plain
SSH, and read the results grouped. They need only key-based SSH to the
targets — no agent, no Nix on the remote side.

```sh
nix shell github:olivecasazza/consortium   # claw, pinch, molt, cast on PATH
```

Every command below runs in CI against real `sshd` hosts through
[`examples/ssh/walkthrough.sh`](../examples/ssh/walkthrough.sh). Point it at
your own hosts to try them:

```sh
NODES='web[01-04]' SSH_USER=ops examples/ssh/walkthrough.sh
```

## Name the hosts: `pinch`

Node sets use bracket notation; `pinch` folds, expands, counts, and combines
them.

```sh
pinch -f node1 node2 node3                  # node[1-3]
pinch -e 'node[1-3]'                        # node1 node2 node3
pinch -c 'node[1-100]'                      # 100
pinch 'node[1-10]' -i 'node[5-15]'          # node[5-10]   intersection
pinch 'node[1-10]' -x 'node[8-99]'          # node[1-7]    exclusion
pinch 'node[1-5]' 'node[3-8]'               # node[1-8]    union
pinch --split 2 'node[1-6]'                 # node[1-3] / node[4-6]
```

Named groups (`-g`, `@group`) resolve through ClusterShell `groups.conf`.

## Run a command: `claw`

```sh
claw -w 'web[01-04]' -- uname -n                     # one line per host
claw -b -w 'web[01-04]' -- uname -r                  # group identical output
claw -l root -f 32 -w 'web[01-16]' -- systemctl restart nginx
claw --options='-o BatchMode=yes' -w 'web[01-04]' -- uptime
claw -S -q -w 'web[01-04]' -- test -f /etc/app.conf; echo $?   # worst rc
```

`-f` caps concurrent connections, `-l` sets the remote user, `-t` / `-u` are
connect and command timeouts, and `--options` passes ssh options through.
Put `--` before the remote command so its flags are not read as `claw`'s.
Targets that are all `localhost` run through the local exec worker; `-R ssh`
forces SSH.

## Copy files: `claw --copy` / `--rcopy`

```sh
claw -w 'web[01-04]' --copy ./app.conf --dest /etc/app/      # push
claw -w 'web[01-04]' --rcopy /var/log/app.log --dest ./logs  # pull
```

`--copy` defaults `--dest` to the source's directory. `--rcopy` writes one
file per host, named `<file>.<host>`, into `--dest` (default: the current
directory). Destination directories must already exist.

## Gather output: `molt`

`molt` reads `host: line` output — from `claw`, `pdsh`, or a log — and groups
hosts that produced identical output:

```sh
claw -w 'web[01-04]' -- uname -r | molt -b
```

`-L` switches to line mode, `-S` changes the key separator.

## From Rust

[`examples/rust/fanout_ssh.rs`](../examples/rust/fanout_ssh.rs) schedules the
same SSH fan-out on a `Task` and reads return codes and per-host output back;
the walkthrough runs it too. See [the library guide](library.md) for the
other examples.
