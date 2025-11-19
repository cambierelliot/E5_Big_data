# Rapport - Assignment 03: Analyzing Graphs + Data Mining/ML

**Auteur :** Owen BRAUX et Elliot CAMBIER 
**Cours :** Big Data Analytics (ESIEE 2025-2026)

---

## Introduction

Ce document présente le travail réalisé pour l'Assignment 03. Ce laboratoire combine deux domaines majeurs du Big Data : l'analyse de graphes distribuée (PageRank) et l'apprentissage automatique (Classification Spam avec SGD).

Les objectifs principaux étaient :

1.  **Graphes (Partie A) :** Implémenter PageRank et Personalized PageRank (PPR) de manière itérative sur PySpark, en optimisant le partitionnement pour minimiser le trafic réseau.
2.  **Machine Learning (Partie B) :** Implémenter un classifieur de Spam "from scratch" (Stochastic Gradient Descent) via une architecture à **Single Reducer**, puis comparer ses performances avec un prédicteur distribué.

---

## Partie A : Analyse de Graphe (PageRank & PPR)

### A.1 - PageRank Itératif

#### Objectif
Calculer le score d'importance de chaque nœud du graphe Gnutella (~6k nœuds) en gérant les "dead-ends" (nœuds sans sortie) et en minimisant le shuffle réseau.

#### Implémentation Technique
-   **Structure RDD :** Le graphe est stocké sous forme de listes d'adjacence `(u, [v1, v2...])`.
-   **Optimisation Partitionnement :** Nous avons utilisé `.partitionBy(4)` sur les RDDs `links` et `ranks`. Cela garantit que les données d'un nœud restent sur la même machine entre les itérations.
-   **Gestion des Dead-Ends :** À chaque itération, la masse perdue est calculée via `subtractByKey` (nœuds présents dans `ranks` mais pas dans `links`) et redistribuée uniformément.
-   **Top-20 :** Utilisation de `.takeOrdered(20)` pour éviter un coûteux `collect()` global sur le driver.

#### Preuves
-   **Plan d'exécution :** `proof/plan_pr.txt`
-   **Capture Spark UI :**

*(On observe une stabilité parfaite du Shuffle Write (~144 KB) à chaque itération `reduceByKey`, preuve que le partitionnement fonctionne).*
![PartieA_PageRang.png](proof%2FPartieA_PageRang.png)
---

### A.2 - Personalized PageRank (PPR) Multi-Sources

#### Objectif
Adapter l'algorithme pour que la "téléportation" (en cas de dead-end ou de saut aléatoire) ne se fasse pas uniformément, mais uniquement vers un set de nœuds sources $S$.

#### Implémentation Technique
-   **Broadcast des Sources :** La liste des sources $S$ est diffusée (`sc.broadcast`) à tous les workers pour un accès rapide sans jointure.
-   **Mise à jour optimisée :** Nous avons utilisé `mapPartitions` avec `preservesPartitioning=True`.
    -   La masse de téléportation est ajoutée **uniquement** si le nœud appartient à $S$.
    -   L'option `preservesPartitioning` indique à Spark que les clés ne changent pas, évitant un re-shuffle inutile entre les itérations.

![PartieA_PPRang.png](proof%2FPartieA_PPRang.png)
---

## Partie B : Spam Classification (SGD & Ensemble)

### B.1 - Entraînement (Le Goulot d'Étranglement)

#### Architecture "Single Reducer"
La consigne imposait de regrouper toutes les données sur une seule tâche pour simuler un apprentissage séquentiel (SGD).
-   **Code :** `.map(lambda x: (0, x)).groupByKey(numPartitions=1)`
-   **Conséquence :** Tout le dataset est transféré (Shuffle Write) vers une seule machine, qui traite 100% des calculs séquentiellement (Gradient Descent).

#### Analyse de Performance
L'impact de cette architecture est visiblement lourd sur les gros datasets :

| Dataset | Input Size | Shuffle Write | Durée (10 époques) | Analyse |
| :--- | :--- | :--- | :--- | :--- |
| **Group X** | ~25.5 MB | ~14.7 MB | ~22 sec | Rapide car le dataset est petit. |
| **Britney** | ~767.8 MB | ~479.8 MB | **~14 min** | **Saturation CPU.** Le reducer unique devient un goulot d'étranglement critique. |

![PartieB_Brit_train.png](proof%2FPartieB_Brit_train.png)
*(Preuve Spark UI : On voit clairement "Tasks: 1/1" et une durée disproportionnée de 14 minutes).*

---

### B.2 - Prédiction (L'Efficacité du Broadcast)

#### Architecture Distribuée
Contrairement à l'entraînement, la prédiction est une tâche "embarrassingly parallel".
-   **Technique :** Le modèle (dictionnaire de poids) est chargé sur le driver puis envoyé via `sc.broadcast()` à tous les workers.
-   **Résultat :** Chaque worker prédit sur sa partition de données localement. **Zéro Shuffle.**

#### Comparaison (Dataset X)
-   **Entraînement :** 22 secondes (avec Shuffle massif).
-   **Prédiction :** **2 secondes** (Shuffle nul).

Cela démontre la puissance de Spark lorsque les tâches sont indépendantes et que les données de référence (le modèle) sont diffusées.

---

### B.3 - Résultats & Évaluation (ROC-AUC)

Nous avons évalué nos modèles en comparant les scores prédits aux labels réels.

| Modèle | Données Testées | Score AUC | Interprétation |
| :--- | :--- | :--- | :--- |
| **Model X** | Group X | **0.9965** | **Overfitting attendu.** Le modèle a mémorisé ses propres données d'entraînement. |
| **Model Britney** | Group X | **0.7687** | **Bonne Généralisation.** Bien qu'entraîné sur des données différentes, il détecte 77% du spam correctement. |
| **Ensemble (Vote)** | Group X | **0.8678** | **Robustesse.** Le vote majoritaire (X + Y) lisse les résultats et offre un excellent compromis. |

#### Étude de Stabilité (Shuffle Study)
Sur 10 essais avec mélange aléatoire des données (shuffle=True), le modèle s'est montré remarquablement stable :

Moyenne AUC : 0.99951

Écart-Type (Std Dev) : 0.00034

Une déviation standard aussi faible (< 0.001) confirme la robustesse de notre implémentation SGD face à l'ordre aléatoire des données.

---

## Métriques & Reproductibilité

- **Environnement :** Détails dans `ENV.md`.
- **Métriques Spark :** Consignées dans `lab_metrics_log.csv`.

#### Metrics Log (Extrait Consolidé)

| run_id | Task | Note | Input Bytes | Shuffle Read | Shuffle Write | Duration |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| A1 | PageRank | 10 iters | 188 KB | 1.2 MB | 144 KB | 41.8s |
| A2 | PPR | Sources=5 | 188 KB | 953 KB | 147 KB | 33.0s |
| B1 | Train_X | Single Reducer | 26.7 MB | 15.4 MB | 15.4 MB | 22s |
| B3 | Train_Britney | Single Reducer | 805 MB | 503 MB | 503 MB | **14min** |
| B4 | Predict_X | Broadcast | 26.7 MB | **0 B** | 0 B | **2s** |

---

## Conclusion Générale

Ce TP a permis de confronter deux paradigmes :

1.  **La gestion du Shuffle (Partie A) :** Dans les algorithmes itératifs de graphes, l'utilisation de `partitionBy` et `preservesPartitioning` est vitale. Sans cela, le graphe serait re-mélangé à travers le réseau à chaque itération (10 fois), ce qui serait désastreux pour la performance.
2.  **Les limites du parallélisme (Partie B) :** L'entraînement SGD a illustré qu'on ne peut pas tout paralléliser simplement. La contrainte mathématique du SGD (mise à jour séquentielle des poids) nous a forcés à centraliser les données (`groupByKey(1)`), créant un goulot d'étranglement majeur (14 minutes sur Britney). À l'inverse, la prédiction, totalement parallélisable via le `Broadcast`, s'exécute quasi-instantanément (2 secondes).