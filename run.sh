#!/bin/bash

mkdir log | true

REPO="https://github.com/fdebotfairbanks/int-ubisoft-rgw-compare"

./probe.py --method GET --timeout 3 $REPO

if [ -e "http" ]; then

    # Check if current directory is a git repository
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        echo "Git repository detected in current directory."
        # Try to pull; if it fails, force-reset tracked files only
        if ! git pull --rebase; then
            echo "git pull failed. Resetting tracked files..."
            CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
            git fetch origin
            git reset --hard origin/"$CURRENT_BRANCH"
            # NOTE: untracked files remain untouched
        fi
    else
        echo "Not a git repository. Initializing..."
        git init
        git remote add origin "$REPO_URL"
        git fetch origin
        git checkout -f -B master origin/master  # or origin/main if needed
    fi

    # Pull docker image

    docker pull fdebot42on/mypython
fi

docker run -it --rm -v $(pwd):/script \
-v /etc/ceph:/etc/ceph \
-v /container/omapdata:/script/omapdata \
--network host \
fdebot42on/mypython python3 /script/$1 "${@:2}" 2>&1 | tee log/log_$(date +%Y%m%d_%H%M%S).log