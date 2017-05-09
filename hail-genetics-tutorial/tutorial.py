# # Hail Tutorial
#
# _This is the [Hail Tutorial](https://hail.is/hail/tutorial.html), lightly reformatted to run in Cloudera Data Science Workbench._
# 
# In this tutorial, we will analyze data from the final phase of the [1000 Genomes Project](http://www.internationalgenome.org/about), as described in [A global reference for human genetic variation (Nature 2015)](http://www.nature.com/nature/journal/v526/n7571/full/nature15393.html). We have downsampled the dataset to approximately 10,000 variants consisting of both rare and common variants. We obtained sample information (population, super-population, sex) from the [1000 Genomes website](http://www.internationalgenome.org/data-portal/sample).
# 
# In the interactive workbench session, type these commands:
# 
# 

import sys
sys.path.append('/home/cdsw/hail-genetics-tutorial/hail-python.zip')

from hail import *
hc = HailContext()

# 
# If there is no error, you're ready to start using Hail! Otherwise, make sure that the `export` variables are correctly set and appropriate versions of all dependencies are installed.
# 
# Before using Hail, let's import the following Python libraries for use throughout the tutorial. Installing and importing [seaborn](http://seaborn.pydata.org/installing.html) is optional; it just makes the plots prettier.
# 

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import seaborn
from math import log, isnan

# 
# ## Import data
# 
# We must first import variant data into Hail's internal format of Variant Dataset (VDS). We use the `import_vcf` method on `HailContext` to load the downsampled 1000 Genomes VCF into Hail. The VCF file is block-compressed (`.vcf.bgz`) which enables Hail to read the file in parallel. Reading files that have not been block-compressed (`.vcf`, `.vcf.gz`) is _significantly_ slower and should be avoided (though often `.vcf.gz` files are in fact block-compressed, in which case renaming to `.vcf.bgz` solves the problem).
# 
# 

vds = hc.import_vcf('Hail_Tutorial-v2/1000Genomes_248samples_coreExome10K.vcf.bgz')

# 
# We next use the `split_multi` method on `dataset` to split multi-allelic variants into biallelic variants. For example, the variant `1:1000:A:T,C` would become two variants: `1:1000:A:T` and `1:1000:A:C`.
# 
# 

vds = vds.split_multi()

# 
# We next use the `annotate_samples_table` method to load phenotypic information on each sample from the sample annotations file.
# 
# 

vds = vds.annotate_samples_table('Hail_Tutorial-v2/1000Genomes.sample_annotations',
                                 root='sa.pheno', 
                                 sample_expr='Sample', 
                                 config=TextTableConfig(impute=True))

# 
# Here `1000Genomes.sample_annotations` refers to the sample annotation data file. `!` is a handy Cloudera Data Science Workbench magic command that allows you to peek at this file without leaving the interpreter.
# 
# 

! hadoop fs -cat Hail_Tutorial-v2/1000Genomes.sample_annotations | head

# 
# The `root` argument says where to put this data. For sample annotations, the root must start with `sa` followed by a `.` and the rest is up to you, so let's use `sa.pheno`.
# 
# The `sample_expr` argument indicates that the sample ID is in column `Sample`.
# 
# The object `TextTableConfig` allows users to provide information about column data types, header existence, comment characters, and field delimiters. Here we'd like to store columns 'isFemale' and 'PurpleHair' as Booleans and the 'CaffeineConsumption' column as Doubles (floating-point). We could do so by passing an explicit type string to `TextTableConfig` of the form 'isFemale: Boolean, PurpleHair: Boolean, CaffeineConsumption: Boolean'.  Instead, we pass `impute=True` to infer column types automatically.
# 
# Lastly, we'll write the dataset to disk so that all future computations begin by reading in the fast VDS rather than the slow VCF.
# 
# 

#out_path = '1kg.vds'
#vds.write(out_path)

# 
# ## Start exploring
# 
# Now we're ready to start exploring! Let's practice reading the vds back in:
# 
# 

#vds = hc.read(out_path)

# 
# First, we'll print some statistics about the size of the dataset using `count`:
# 
# 

vds.count()

# 
# If the Boolean parameter `genotypes` is set to `True`, the overall call rate across all genotypes is computed as well:
# 
# 

vds.count(genotypes=True)

# 
# So the call rate before any QC filtering is about 98.7%.
# 
# Let's print the types of all annotations.
# 
# 

print('variant annotations:')
print(vds.variant_schema)
print('\nsample annotations:')
print(vds.sample_schema)
print('\nglobal annotations:')
print(vds.global_schema)

# 
# Note the annotations imported from the original VCF, as well as the sample annotations added above. Notice how those six sample annotations loaded above are nested inside `sa.pheno` as defined by the `root` option in `annotate_samples table`.
# 
# Next we'll add some global annotations including the list of populations that are present in our dataset and the number of samples in each population, using the Hail expression language and the `query_samples` method. The 1000 Genomes Super-Population codings are:
# 
#   - SAS = South Asian
#   - AMR = Americas
#   - EUR = European
#   - AFR = African
#   - EAS = East Asian
# 
# We'll first build up a list of query expressions, then evaluate them all at once to save time.
# 
# 

expressions = [ 
  'samples.map(s => sa.pheno.Population).collect().toSet()',
  'samples.map(s => sa.pheno.SuperPopulation).collect().toSet()',
  'samples.filter(s => sa.pheno.PurpleHair).count()',
  'samples.filter(s => !sa.pheno.PurpleHair).count()' ]
[populations, super_populations, case_count, control_count] = vds.query_samples(expressions)

print('populations = %s' % populations)
print('super populations = %s' % super_populations)
print('case count = %s' % case_count)
print('control count = %s' % control_count)
print('total samples = %s' % vds.num_samples)

# 
# Now it's easy to count samples by population using the `counter()` aggregator:
# 
# 

[counter] = vds.query_samples(['samples.map(s => sa.pheno.SuperPopulation).counter()'])
for k, v in counter.iteritems():
    print('population %s found %s times' % (k, v))

# 
# ## Quality control (QC)
# 
# Before running genotype-phenotype association tests, we better clean up the raw data! We'll filter out:
# 
# - genotypes that don't have strong evidence supporting the genotype call
# - samples that are outliers on key summary statistics across the dataset
# - variants with low mean genotype quality or out of [Hardy-Weinberg equilibrium](https://en.wikipedia.org/wiki/Hardy–Weinberg_principle).
# 
#
# The QC procedures below are a sampler covering various features of Hail, not an optimal pipeline for your research.
# 
# For filtering, we make extensive use of the [Hail expression language](https://hail.is/hail/exprlang.html). Here `g` is genotype, `v` is variant, `s` is sample, and annotations are accessible via `va`, `sa`, and `global`. 
# 
# ### Filter genotypes
# 
# Let's filter genotypes based on allelic balance using the `filter_genotypes` method.
# 
# 

filter_condition_ab = '''let ab = g.ad[1] / g.ad.sum() in
                         ((g.isHomRef() && ab <= 0.1) || 
                          (g.isHet() && ab >= 0.25 && ab <= 0.75) || 
                          (g.isHomVar() && ab >= 0.9))'''
vds_gAB = vds.filter_genotypes(filter_condition_ab)

# 
# In this code, we first construct an expression  `filter_condition_ab` that evaluates to a Boolean. We use `let ... in` syntax to define a temporary variable `ab` for the allelic balance which is calculated from the allelic depth `g.ad`, a zero-indexed array (so `g.ad[0]` and `g.ad[1]` are read counts for reference allele and unique alternate allele, respectively; this dataset is bi-allelic, but Hail supports multi-allelic variants as well). We require for homozygous calls that the allelic balance be within `.1` of the expected mode, and that for heterozygote calls (`g.isHet()`) the allelic balance be between 0.25 and 0.75. Additional methods on genotype are documented [here](https://hail.is/hail/types.html#genotype).
# 
# 

vds_gAB.count(genotypes=True)

# 
# Now the call rate is about 95%, so nearly 4% of genotypes failed the filter. Filtering out a genotype is equivalent to setting the genotype call to missing.
# 
# ### Filter samples
# 
# Having removed suspect genotypes, let's next remove variants with low call rate and then calculate summary statistics per sample with the `sample_qc` method.
# 
# 

vds_gAB_vCR = (vds_gAB
    .filter_variants_expr('gs.fraction(g => g.isCalled()) > 0.95')
    .sample_qc())

# 
# The call rate for each variant is calculated using the `fraction` [aggregable](https://hail.is/hail/types.html#aggregable-genotype) on the genotypes `gs`. `sample_qc` adds a number of statistics to sample annotations:
# 
# 

print(vds_gAB_vCR.sample_schema)

# 
# Let's export these sample annotations to a text file and take a look at them:
# 
# 

vds_gAB_vCR.export_samples('file:///tmp/sampleqc.txt', 'Sample = s.id, sa.qc.*')

# 
# 

! head /tmp/sampleqc.txt | cut -f 1-10

# 
# We can further analyze these results locally using Python's [matplotlib](http://matplotlib.org/) library. Below is an example plot of two variables (call rate and meanGQ), along with the code that generate the plot.
# (Note the code is wrapped in a function to avoid a limitation in Workbench, which otherwise shows multiple plots.)
# 

def showplot1():
    sampleqc_table = vds_gAB_vCR.samples_keytable().to_pandas()

    plt.subplot(1, 2, 1)
    plt.hist(sampleqc_table["sa.qc.callRate"], bins=np.arange(.75, 1.01, .01))
    plt.xlabel("Call Rate")
    plt.ylabel("Frequency")
    plt.xlim(.75, 1)
    plt.axvline(.97, color='r')
    
    plt.subplot(1, 2, 2)
    plt.hist(sampleqc_table["sa.qc.gqMean"], bins = np.arange(0, 105, 5))
    plt.xlabel("Mean Sample GQ")
    plt.ylabel("Frequency")
    plt.xlim(0, 105)
    plt.axvline(20, color = 'r')
    
    #plt.tight_layout()
    plt.show()

showplot1()

# 
# Let's remove the samples that are outliers in the plots above, where cutoffs are given by the red lines. We'll remove these samples from `vds_gAB` (after filtering genotypes but before filtering variants) because it's possible that poor-quality samples decreased the call rate on variants we'd actually like to keep. Here is one of the many ways we could do this step:
# 
# 

vds_gAB_sCR_sGQ = (vds_gAB
    .annotate_samples_vds(vds_gAB_vCR, code = 'sa.qc = vds.qc' )
    .filter_samples_expr('sa.qc.callRate >= 0.97 && sa.qc.gqMean >= 20'))

# 
# As before, let's use the `annotate_global_expr_by_sample` method to count the number of samples by phenotype that remain in the dataset after filtering.
# 
# 

post_qc_exprs = [
    'samples.filter(s => sa.pheno.PurpleHair).count()',
    'samples.filter(s => !sa.pheno.PurpleHair).count()' ]
[num_cases, num_controls] = vds_gAB_sCR_sGQ.query_samples(post_qc_exprs)
print('case count is %s, control count is %s' % (num_cases, num_controls))

# 
# ### Filter variants
# 
# We now have `vds_gAB_sCR_sGQ`, a VDS where low-quality genotypes and samples have been removed.
# 
# Let's use the `variant_qc` method to start exploring variant metrics:
# 
# 

vds_gAB_sCR_sGQ = vds_gAB_sCR_sGQ.variant_qc()

print(vds_gAB_sCR_sGQ.variant_schema)

# 
# We've once again used matplotlib to make histograms of four summary statistics (call rate, allele frequency, mean GQ, and Hardy Weinberg Equilibrium p-value. Notice how the histogram for HWE is massively inflated for small p-values. This is because we calculated HWE p-values with all five populations lumped together.
# 
# 

def showplot2():
    variantqc_table = vds_gAB_sCR_sGQ.variants_keytable().to_pandas()

    plt.subplot(2, 2, 1)
    variantgq_means = variantqc_table["va.qc.gqMean"]
    plt.hist(variantgq_means, bins = np.arange(0, 85, 5))
    plt.xlabel("Variant Mean GQ")
    plt.ylabel("Frequency")
    plt.xlim(0, 80)
    plt.axvline(20, color = 'r')
    
    plt.subplot(2, 2, 2)
    variant_mleaf = variantqc_table["va.qc.AF"]
    plt.hist(variant_mleaf, bins = np.arange(0, 1.05, .05))
    plt.xlabel("Minor Allele Frequency")
    plt.ylabel("Frequency")
    plt.xlim(0, 1)
    plt.axvline(0.05, color = 'r')
    
    plt.subplot(2, 2, 3)
    plt.hist(variantqc_table['va.qc.callRate'], bins = np.arange(0, 1.05, .05))
    plt.xlabel("Variant Call Rate")
    plt.ylabel("Frequency")
    plt.xlim(.5, 1)
    
    plt.subplot(2, 2, 4)
    plt.hist(variantqc_table['va.qc.pHWE'], bins = np.arange(0, 1.05, .05))
    plt.xlabel("Hardy-Weinberg Equilibrium p-value")
    plt.ylabel("Frequency")
    plt.xlim(0, 1)
    
    #plt.tight_layout()
    plt.show()

showplot2()
    
# 
# Let's use the `annotate_variants_expr` method to programmatically compute Hardy Weinberg Equilibrium for each population. First, we construct `hwe-expressions`.
# 
# 

hwe_expressions = [
    'va.hweByPop.hweEUR = gs.filter(g => sa.pheno.SuperPopulation == "EUR").hardyWeinberg()',
    'va.hweByPop.hweSAS = gs.filter(g => sa.pheno.SuperPopulation == "SAS").hardyWeinberg()',
    'va.hweByPop.hweAMR = gs.filter(g => sa.pheno.SuperPopulation == "AMR").hardyWeinberg()',
    'va.hweByPop.hweAFR = gs.filter(g => sa.pheno.SuperPopulation == "AFR").hardyWeinberg()',
    'va.hweByPop.hweEAS = gs.filter(g => sa.pheno.SuperPopulation == "EAS").hardyWeinberg()' ]

# 
# We can do this more tersely using list comprehensions in Python; we'll use this approach in the remainder of the tutorial.
# 
# 

populations = ['EUR', 'SAS', 'AMR', 'AFR', 'EAS']
hwe_expressions = ['va.hweByPop.hwe%s = gs.filter(g => sa.pheno.SuperPopulation == "%s").hardyWeinberg()' % (pop, pop) for pop in populations]

# 
# Now we can go ahead and use `annotate_variants_expr`.
# 
# 

vds_gAB_sCR_sGQ = vds_gAB_sCR_sGQ.annotate_variants_expr(hwe_expressions)
vds_gAB_sCR_sGQ.persist()

print(vds_gAB_sCR_sGQ.variant_schema)

# 
# Above, for each variant, we filter the genotypes to only those genotypes from the population of interest using a filter function on the [genotype aggregable](https://hail.is/hail/types.html#aggregable-genotype) and then calculate the Hardy-Weinberg Equilibrium p-value using the `hardyWeinberg()` function on the filtered genotype aggregable.
# 
# The `persist` method caches the dataset in its current state on memory/disk, so that downstream processing will be faster. 
# 
# Printing the schema reveals that we've added new fields to the variant annotations for HWE p-values for each population. We've got quite a few variant annotations now! Notice that the results of these annotation statements are structs containing two elements, `rExpectedHetFrequency` and `pHWE`.
# 
# We can now filter variants based on HWE p-values with respect to each population.
# 
# 
hwe_filter_expression = " && ".join(['va.hweByPop.hwe%s.pHWE > 1e-6' % pop for pop in populations])
vds_gAD_sCR_sGQ_vHWE = vds_gAB_sCR_sGQ.filter_variants_expr(hwe_filter_expression)

print('variants after HWE filter: %d' % vds_gAD_sCR_sGQ_vHWE.count_variants())

# 
# Running `count_variants`, we see that by calculating HWE p-values in each population separately, we keep 10891 variants, filtering out 70.  We would have filtered far more if we used the population-agnostic single statistic!
# 
# Lastly we use the `filter_variants_expr` method to keep only those variants with a mean GQ greater than or equal to 20.
# 
# 

vds_gAD_sCR_sGQ_vHWE_vGQ = vds_gAD_sCR_sGQ_vHWE.filter_variants_expr('va.qc.gqMean >= 20')

print('variants after GQ filter: %d' % vds_gAD_sCR_sGQ_vHWE_vGQ.count_variants())

# 
# We are left with 10662 total variants.
# 
# ### Sex check
# 
# It's *always* a good idea to check that the reported sex of samples is consistent with sex chromosome ploidy estimated directly from genetic data. A high sex-check failure rate would suggest that sample swaps may have occurred.
# 
# There are 273 X chromosome variants in the original dataset:
# 
# 

vds.filter_variants_expr('v.contig == "X"').count_variants()

# 
# However, after variant QC, the number of X chromosome variants has dropped to 164.
# 
# 

vds_gAD_sCR_sGQ_vHWE_vGQ.filter_variants_expr('v.contig == "X"').count_variants()

# 
# Oops! HWE statistics on the X chromosome should ignore male samples, since males have only two possible genotypes (HomRef or HomVar). We're going to have to go back to `vds_gAB_sCR_sGQ` and modify how we calculate HWE. We use a conditional expression on the [Variant object method](https://hail.is/hail/types.html#variant) `v.isAutosomal` so that variants on the X chromosome will only include female samples in the calculation. We can also use the same `hwe_filter_expression` from above.
# 
# 

sex_aware_hwe_exprs = [
'''va.hweByPop.hwe{pop} = 
if (v.isAutosomal())
  gs.filter(g => sa.pheno.SuperPopulation == "{pop}").hardyWeinberg()
else
  gs.filter(g => sa.pheno.SuperPopulation == "{pop}" && sa.pheno.isFemale).hardyWeinberg()'''.format(pop = p) for p in populations]

vds_filtered = (vds_gAB_sCR_sGQ
    .annotate_variants_expr(sex_aware_hwe_exprs)
    .filter_variants_expr(hwe_filter_expression + '&& va.qc.gqMean >= 20'))
vds_filtered.persist()

print('total variants = %s' % vds_filtered.count_variants())
print('X chromosome variants = %s' % vds_filtered.query_variants(['variants.filter(v => v.contig == "X").count()'])[0])

# 
# For sex check, we first use the `impute_sex` method with a minimum minor allele frequency threshold `maf_threshold` argument of 0.05 to determine the genetic sex of each sample based on the inbreeding coefficient. `impute_sex` adds the Boolean sample annotation `sa.imputesex.isFemale` and we then create a new Boolean sample annotation `sa.sexcheck` which indicates whether the imputed sex `sa.imputesex.isFemale` is the same as the reported sex `sa.pheno.isFemale`.
# 
# 

vds_sex_check = (vds_filtered
    .impute_sex(maf_threshold=0.05)
    .annotate_samples_expr('sa.sexcheck = sa.pheno.isFemale == sa.imputesex.isFemale'))
total_samples = vds_sex_check.num_samples
sex_check_passes = vds_sex_check.filter_samples_expr('sa.sexcheck').num_samples

print('total samples: %s' % total_samples)
print('sex_check_passes: %s' % sex_check_passes)

# 
# We see that the genetic sex does not match the reported sex for 567 samples, an extremely high sex check failure rate! To figure out why this happened, we can use a Hail expression to look at the values that `sa.sexcheck` takes.
# 
# 

[counter] = vds_sex_check.query_samples(['samples.map(s => sa.sexcheck).counter()'])
for k, v in counter.iteritems():
    print('population %s found %s times' % (k, v))

# 
# Aha! While we only have 1 'false' sex-check values, we have 71 missing sex-check values. Since `pheno.isFemale` is never missing in the sample annotations file, this means that there were 71 samples that could not be confidently imputed as male or female. This is because in our small dataset the number of variants on the X chromosome (about 200) is not sufficient to impute sex reliably. Let's instead keep those samples with missing sex-check.
# 
# 

vds_QCed = vds_sex_check.filter_samples_expr('sa.sexcheck || isMissing(sa.sexcheck)')
vds_QCed.persist()

print('samples after filter: %s' % vds_QCed.num_samples)

# 
# ## PCA
# 
# To account for population stratification in association testing, we use principal component analysis to compute features that are proxies for genetic similarity. PCA is typically performed on variants in linkage equilibrium. The text file *purcell5k.interval_list* contains a list of such independent common variants.
# 
# To calculate principal components, we first use the `filter_variants_intervals` method to filter down to SNPs from this list. Next, we use the `pca` method to calculate the first 10 principal components (10 is the default number). The results are stored as sample annotations with root given by the `scores` parameter.
# 
# 

vds_pca = (vds_QCed.filter_variants_intervals('Hail_Tutorial-v2/purcell5k.interval_list')
    .pca(scores='sa.pca'))

# 
# We can then make a Python plot of the samples in PC space colored by population group:
# 
# 

def showplot3():
    pca_table = vds_pca.samples_keytable().to_pandas()
    colors = {'AFR': 'black', 'AMR': 'red', 'EAS': 'green', 'EUR': 'blue', 'SAS': 'cyan'}
    plt.scatter(pca_table["sa.pca.PC1"], pca_table["sa.pca.PC2"], c = pca_table["sa.pheno.SuperPopulation"].map(colors), alpha = .5)
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    legend_entries = [mpatches.Patch(color= c, label=pheno) for pheno, c in colors.items()]
    plt.legend(handles=legend_entries)
    plt.show()
    
showplot3()

# 
# ## Association testing
# 
# Now that we have a clean dataset with principal component annotations, let's test for association between genetic variation and the phenotypes CaffeineConsumption (continuous) and PurpleHair (dichotomous).
# 
# ### Linear regression with covariates
# 
# Let's run linear regression on `vds_QCed`. First, we will filter to variants with a allele frequency between 5% and 95%. Next, we use the `linreg` method, specifying the response variable `y` to be the sample annotation `sa.pheno.CaffeineConsumption`. We use four sample covariates in addition to the (implicit) intercept: `sa.pca.PC1`, `sa.pca.PC2`, `sa.pca.PC3`, `sa.pheno.isFemale`.
# 
# 

vds_gwas = (vds_QCed
    .filter_variants_expr('va.qc.AF > 0.05 && va.qc.AF < 0.95')
    .annotate_samples_vds(vds_pca, code='sa.pca = vds.pca')
    .linreg('sa.pheno.CaffeineConsumption', 
            covariates=['sa.pca.PC1', 'sa.pca.PC2', 'sa.pca.PC3', 'sa.pheno.isFemale']))

# 
# The results of linear regression are stored as variant annotations and can be accessed with the root name `va.linreg`. To check for p-value inflation and significant associations, let's create a log-scaled [Q-Q plot](https://en.wikipedia.org/wiki/Q%E2%80%93Q_plot). We'll be making such plots a few times, so let's first define a function for this purpose.
# 
# 

def qqplot(pvals, xMax, yMax):
    spvals = sorted(filter(lambda x: not(isnan(x)), pvals))
    exp = [-log(float(i) / len(spvals), 10) for i in np.arange(1, len(spvals) + 1, 1)]
    obs = [-log(p, 10) for p in spvals]
    plt.scatter(exp, obs)
    plt.plot(np.arange(0, max(xMax, yMax)), c="red")
    plt.xlabel("Expected p-value (-log10 scale)")
    plt.ylabel("Observed p-value (-log10 scale)")
    plt.xlim(0, xMax)
    plt.ylim(0, yMax)
    plt.show()

# 
# With this new function, we can make a Q-Q plot for our linear regression as follows.
# 
# 

linreg_pvals = sorted(vds_gwas.variants_keytable().to_pandas()["va.linreg.pval"])
qqplot(linreg_pvals, 5, 6)

# 
# ### Logistic regression with covariates
# 
# We continue from `vds_gwas`. The logistic regression method also takes a test type argument. We will use the Wald test.
# 
# 

vds_gwas = (vds_gwas
    .logreg(test='wald', y='sa.pheno.PurpleHair',
            covariates=['sa.pca.PC1', 'sa.pca.PC2', 'sa.pca.PC3', 'sa.pheno.isFemale']))

# 
# We can reuse our Q-Q plot function:
# 
# 

logreg_pvals = vds_gwas.variants_keytable().to_pandas()["va.logreg.pval"]
qqplot(logreg_pvals, 5, 6)

# 
# ### Fisher's Exact Test for Rare Variants
# 
# We'll start with `vds_QCed` here (our `vds_gwas` isn't appropriate for rare variant tests because we filtered them all out!). This time we filter to rare variants (allele frequency less than 5% or greater than 95%). Next we annotate variants with four counts about the aggregate statistics of the samples at each position. These new variant annotations can be used as inputs to [Fisher's Exact Test](https://en.wikipedia.org/wiki/Fisher's_exact_test) test which takes as input four integers representing a 2 x 2 contingency table. We define the variant annotation `va.fet` to be the output of the `fet` function in the [expression language](https://hail.is/expr_lang.html). The results are stored in vds_fet with Q-Q plot below.
# 
# 

rare_variant_annotations = [
    '''va.minorCase = 
        gs.filter(g => sa.pheno.PurpleHair && g.isHet()).count() +
        2 * gs.filter(g => sa.pheno.PurpleHair && g.isHomVar()).count()''',
    '''va.minorControl = 
        gs.filter(g => !sa.pheno.PurpleHair && g.isHet()).count() + 
        2 * gs.filter(g => !sa.pheno.PurpleHair && g.isHomVar()).count()''',
    '''va.majorCase = 
        gs.filter(g => sa.pheno.PurpleHair && g.isHet()).count() +
        2 * gs.filter(g => sa.pheno.PurpleHair && g.isHomRef()).count()''',
    '''va.majorControl = 
        gs.filter(g => !sa.pheno.PurpleHair && g.isHet()).count() +
        2 * gs.filter(g => !sa.pheno.PurpleHair && g.isHomRef()).count()''' ]

vds_fet = (vds_QCed
    .filter_variants_expr('va.qc.AF <= 0.05 || va.qc.AF >= 0.95')
    .annotate_variants_expr(rare_variant_annotations)
    .annotate_variants_expr('''va.fet = 
                                  fet(va.minorCase.toInt(), va.minorControl.toInt(),
                                      va.majorCase.toInt(), va.majorControl.toInt())'''))

fet_pvals = vds_fet.variants_keytable().to_pandas()["va.fet.pValue"]
qqplot(fet_pvals, 5, 6)

# 
# ## Eplilogue
# 
# Congrats! If you've made it this far, you're perfectly primed to read the [Overview](https://hail.is/hail/overview.html), look through the [Hail objects](https://hail.is/hail/types.html) representing many core concepts in genetics, and check out the many Hail functions defined in the [Python API](https://hail.is/hail/api.html). As you use Hail for your own science, we'd love to hear from you on [Gitter chat](https://gitter.im/hail-is/hail) or the [discussion forum](http://discuss.hail.is).
# 
# For reference, here's the full workflow to all tutorial endpoints combined into one script, assuming you've already created the expression strings like `filter_condition_ab` above.
# 
# 

vds_gAB_vCR = (hc.import_vcf('Hail_Tutorial-v2/1000Genomes_248samples_coreExome10K.vcf.bgz')
         .split_multi()
         .annotate_samples_table('Hail_Tutorial-v2/1000Genomes.sample_annotations',
                                 root='sa.pheno', 
                                 sample_expr='Sample', 
                                 config=TextTableConfig(impute=True))
         .filter_genotypes(filter_condition_ab)
         .filter_variants_expr('gs.fraction(g => g.isCalled()) > 0.95')
         .sample_qc())

vds_QCed = (vds_gAB_vCR
    .annotate_samples_vds(vds_gAB_vCR, code = 'sa.qc = vds.qc')
    .filter_samples_expr('sa.qc.callRate >= 0.97 && sa.qc.gqMean >= 20')
    .variant_qc()
    .annotate_variants_expr(sex_aware_hwe_exprs)
    .filter_variants_expr(hwe_filter_expression + '&& va.qc.gqMean >= 20')
    .impute_sex(maf_threshold=0.05)
    .annotate_samples_expr('sa.sexcheck = sa.pheno.isFemale == sa.imputesex.isFemale')
    .filter_samples_expr('sa.sexcheck || isMissing(sa.sexcheck)'))

vds_pca = (vds_QCed.filter_variants_intervals('Hail_Tutorial-v2/purcell5k.interval_list')
    .pca(scores='sa.pca'))

vds_gwas = (vds_QCed
    .filter_variants_expr('va.qc.AF > 0.05 && va.qc.AF < 0.95')
    .annotate_samples_vds(vds_pca, code='sa.pca = vds.pca')
    .linreg('sa.pheno.CaffeineConsumption', 
            covariates=['sa.pca.PC1', 'sa.pca.PC2', 'sa.pca.PC3', 'sa.pheno.isFemale'])
    .logreg(test='wald', y='sa.pheno.PurpleHair',
            covariates=['sa.pca.PC1', 'sa.pca.PC2', 'sa.pca.PC3', 'sa.pheno.isFemale']))

vds_fet = (vds_QCed
    .filter_variants_expr('va.qc.AF <= 0.05 || va.qc.AF >= 0.95')
    .annotate_variants_expr(rare_variant_annotations)
    .annotate_variants_expr('''va.fet = fet(va.minorCase.toInt(), va.minorControl.toInt(),
                                            va.majorCase.toInt(), va.majorControl.toInt())'''))
