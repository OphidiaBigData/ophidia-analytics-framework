How To Contribute
=================

Every open source project lives from the generous help by contributors that sacrifice their time and Ophidia is no different.

Here are a few hints and rules to get you started:

- Add yourself to the AUTHORS.md file in an alphabetical fashion.
  Every contribution is valuable and shall be credited.
- If your change is noteworthy, add an entry to the HISTORY.md file.
- No contribution is too small; please submit as many fixes for typos and grammar bloopers as you can!
- Try to keep backward compatibility.
- Always add docs or comments for your code.
  If a feature is not documented, it doesn’t exist.
- Write good commit messages.
- Ideally, make your pull requests just one commit.
- If your pull request contains changes to any Autotools configuration file (like configure.ac), please repeat the Autotools procedure</br>
<code>
aclocal -I m4</br>
autoconf</br>
libtoolize</br>
autoheader</br>
automake</br>
</code>
in order to generate and include into the commit the <code>configure</code> executable and all <code>*.in</code> configuration files.
- Do not include configuration files (even the .gitignore) with personal settings.
- Do not include external and/or hidden files originating from any IDE.
- Do not include the <code>autom4te.cache</code> directory.
- Modify the software version inside the <code>configure.ac</code> and/or inside the code.
- Remember to update the README.md and the NOTICE.md files with any new library dependency.

<b>Note</b>:</br>
If you have something great but aren’t sure whether it adheres -- or even can adhere -- to the rules above: please submit a pull request anyway!</br>
In the best case, we can mold it into something, in the worst case the pull request gets politely closed.
There’s absolutely nothing to fear.

Thank you for considering to contribute to Ophidia!
