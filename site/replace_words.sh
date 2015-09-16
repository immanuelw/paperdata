grep -lr --exclude-dir=".git" -e "paper" . | xargs sed -i "s/paper/paper/g"
