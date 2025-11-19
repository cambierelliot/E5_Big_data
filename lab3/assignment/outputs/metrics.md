
# Big Data Analytics - Assignment 03 Metrics
**Author:** Badr TAJINI
**Date:** 2025-11-18 11:41:00

## 1. Spam Classification Performance (ROC-AUC)
* **Model Group X (Self-Test):** 0.99651
* **Model Britney (Generalization on X):** 0.76866
* **Ensemble Vote (X+Y on X):** 0.86781

## 2. Shuffle Study (Stability Analysis)
* **Dataset Used:** /mnt/c/Users/ellio/OneDrive/Documents/Postbac/E5-DSIA/elliot/big data/bigdata/lab3/assignment/data/spam/spam.train.group_y.txt
* **Trials:** 10
* **Mean AUC:** 0.99951
* **Std Deviation:** 0.00034
* **Raw Scores:** [0.9988969318494189, 0.9990449473840016, 0.9995348083198827, 0.9998061701332845, 0.9992775432240604, 0.9998942746181552, 0.9997568316217568, 0.9995030907053294, 0.9998414119272329, 0.999506614884724]

> *Analysis: A low standard deviation indicates that the SGD algorithm is robust to the order of input data.*
