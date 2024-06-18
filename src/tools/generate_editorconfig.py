#!/usr/bin/env python3

import os

def cd_to_repo_root():
    abspath = os.path.abspath(__file__)
    dname = os.path.join(os.path.dirname(abspath), '..', '..')
    os.chdir(dname)

def main():
    cd_to_repo_root()

    with open(".gitattributes", "r") as f:
        lines = f.read().splitlines()

    new_contents = """root = true

[*]
indent_size = tab
"""

    for line in lines:
        if line.startswith("#") or len(line) == 0:
            continue
        name, git_rules = line.split()
        if git_rules == "-whitespace":
            rules = [
                "indent_style = unset",
                "indent_size = unset",
                "trim_trailing_whitespace = unset",
                "insert_final_newline = unset",
            ]
        elif git_rules.startswith("whitespace="):
            git_whitespace_rules = git_rules.replace("whitespace=", "").split(",")
            rules = []
            if '-blank-at-eol' in git_whitespace_rules:
                rules += ['trim_trailing_whitespace = unset']
            else:
                rules += ["trim_trailing_whitespace = true"]

            if "-blank-at-eof" in git_whitespace_rules:
                rules += ["insert_final_newline = unset"]
            else:
                rules += ["insert_final_newline = true"]

            if "tab-in-indent" in git_whitespace_rules:
                rules += ["indent_style = space"]
            elif "indent-with-non-tab" in git_whitespace_rules:
                rules += ["indent_style = tab"]
            else:
                rules += ["indent_style = unset"]

            tab_width = '8'
            for rule in git_whitespace_rules:
                if rule.startswith('tabwidth='):
                    tab_width = rule.replace('tabwidth=', '')
            rules += [f'tab_width = {tab_width}']

        else:
            continue

        rules = '\n'.join(rules)
        new_contents += f"\n[{name}]\n{rules}\n"

    new_contents += """
# We want editors to use tabs for indenting Perl files, but we cannot add it
# such a rule to .gitattributes, because certain lines are still indented with
# spaces (e.g. SYNOPSIS blocks).
[*.{pl,pm}]
indent_style = tab
"""

    with open(".editorconfig", "w") as f:
        f.write(new_contents)




if __name__ == "__main__":
    main()
