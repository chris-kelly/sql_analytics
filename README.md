<h1>SQL Analytics</h1>

This is a repository to show that analytical things can be done in SQL :P

So far it consists of two sub-folders:

<h3>Cosine similarity between text strings:</h3>
<ul>
<li>Calculates the cosine similarity between every combination of two tables of strings.</li>
<li>It fits TF-IDF for tokenisation (unigram, bigram, trigram, bigram-skip) for one column, and transforms the other.</li>
<li>It then takes advantage of L2-normalisation as a cheeky math hack to use a linear kernel instead to calculate cosine similarity.</li>
<li>So it essentially just creates a sum product on the left-join to get a dot product :D</li>
</ul>

<h3>Weighted Least Squares UDFs:</h3>
<ul>
<li>Calculates the alpha and beta required for a univariate weighted least squares regression</li>
<li>UDFs include partition windows so multiple models can be trained on different cuts of the same table (and tested on other cuts!)</li>
<li>Added example comments to show how to extend to multivariate regressions if required</li>
<li>Also includes MAE and MAPE calculations to assess performance on training data</li>
</ul>
