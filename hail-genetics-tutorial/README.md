# Hail tutorial

[Hail](https://hail.is) is an open-source, scalable framework for exploring and analyzing genetic data.
This repo contains the [Hail Tutorial](https://hail.is/hail/tutorial.html), lightly reformatted to run in Cloudera Data Science Workbench.

To run, open _tutorial.py_ in Cloudera Data Science Workbench and click the run icon.

Once you've finished, run the _cleanup.sh_ script to remove data created by the tutorial.

### How the CDSW version was created

The original Hail tutorial IPython notebook was converted to markdown using the following commands.

```bash
ipython nbconvert --to markdown python/hail/docs/tutorial.ipynb
sed -i -e 's/^/# /' tutorial.md
```

Then the contents of _tutorial.md_ was pasted into _tutorial.py_, and Python code sections
(in triple backticks) were converted to be simple inline code, with no markup. Shell commands were
converted to use the `! <command>` syntax. 