#!/usr/bin/env bash
# Build the LOO + boundary-pair kube-apiserver binaries for the attribution matrix.
set -u
WT=/home/jpbetz/projects/k8s-worktrees/loo
BINS=/home/jpbetz/projects/k8s-worktrees/bins
build() { # name
  go build -C "$WT/cmd/kube-apiserver" -o "$BINS/kube-apiserver-$1" . && echo "BUILT $1" || echo "FAILED $1"
}
cd "$WT"
git reset --hard HEAD >/dev/null && git clean -fd >/dev/null 2>&1

# LOO arms (HEAD minus one change, clean reverts)
git revert --no-commit 8059a7316b6 && build no-presize; git reset --hard HEAD >/dev/null
git revert --no-commit b8a20977dbf && build no-bufproto; git reset --hard HEAD >/dev/null

# Boundary pairs (commit^ vs commit)
for pair in "pooled:e724a00e536" "table:50945f3595c"; do
  name="${pair%%:*}"; sha="${pair##*:}"
  git checkout --detach "${sha}^" >/dev/null 2>&1 && build "${name}-pre"
  git checkout --detach "$sha"    >/dev/null 2>&1 && build "${name}-post"
done
# lock-contention span: parent of first commit vs second commit
git checkout --detach "d8e0cdaacf3^" >/dev/null 2>&1 && build "locks-pre"
git checkout --detach "1894fe286d9"  >/dev/null 2>&1 && build "locks-post"

git checkout --detach 018fc644372 >/dev/null 2>&1
echo "ALL BUILDS DONE"; ls -la "$BINS"
