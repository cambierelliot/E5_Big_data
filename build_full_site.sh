#!/bin/bash
# Script pour générer un Site Web Complet (Dashboard) avec tous les Labs

QUARTZ_DIR="quartz-min"
REPO_URL="https://github.com/cambierelliot/E5_Big_data"

echo "=== 🚀 Démarrage de la génération du Dashboard Complet ==="

# 1. Installation propre de Quartz
# On supprime l'ancien pour éviter les conflits
rm -rf "$QUARTZ_DIR"
echo ">>> 📦 Clonage de Quartz..."
git clone https://github.com/jackyzha0/quartz.git "$QUARTZ_DIR"
echo ">>> 📦 Installation des dépendances..."
cd "$QUARTZ_DIR" && npm ci && cd ..

# Dossiers de destination dans Quartz
CONTENT_DIR="$QUARTZ_DIR/content"
STATIC_NB_DIR="$CONTENT_DIR/static/nb"
mkdir -p "$STATIC_NB_DIR"

# 2. La boucle magique : Trouver TOUS les notebooks et les ajouter
echo ">>> 🔍 Recherche des notebooks dans les dossiers lab*..."

# On cherche tous les fichiers .ipynb dans les dossiers commençant par 'lab'
find . -type f -path "./lab*/*.ipynb" | while read notebook; do
    # Ex: notebook = ./lab3/assignment/BDA_Assignment03.ipynb
    
    # Nom du fichier sans extension (ex: BDA_Assignment03)
    filename=$(basename "$notebook" .ipynb)
    
    # Chemin du dossier parent (ex: ./lab3/assignment)
    parent_dir=$(dirname "$notebook")
    
    # On nettoie le chemin pour l'arborescence du site (enlever le ./ au début)
    clean_dir=${parent_dir#./}
    
    echo "   ➡️ Traitement de : $clean_dir/$filename"
    
    # A. Convertir le notebook en HTML
    jupyter nbconvert --to html "$notebook" --output-dir "$STATIC_NB_DIR" --template basic
    
    # B. Créer le dossier correspondant dans le contenu du site
    mkdir -p "$CONTENT_DIR/$clean_dir"
    
    # C. Créer la page Markdown qui affiche le notebook
    cat <<EOF > "$CONTENT_DIR/$clean_dir/$filename.md"
---
title: $filename
---
> [📄 Voir le code source sur GitHub]($REPO_URL/blob/main/$clean_dir/$filename.ipynb)

<iframe src="/static/nb/$filename.html" width="100%" height="1200px" style="border:none;"></iframe>
EOF

done

# 3. Ajouter les Rapports Markdown existants (ex: report.md)
echo ">>> 📑 Ajout des fichiers Markdown (rapports)..."
find . -maxdepth 2 -name "*.md" -not -name "README.md" -not -path "./quartz-min/*" | while read mdfile; do
    cp "$mdfile" "$CONTENT_DIR/"
    echo "   ➡️ Ajouté : $mdfile"
done

# 4. Créer la page d'accueil (index.md)
echo ">>> 🏠 Création de la page d'accueil..."
cat <<EOF > "$CONTENT_DIR/index.md"
---
title: Dashboard Big Data Analytics
---

# Bienvenue sur le rapport de projet

Ce site regroupe l'ensemble des travaux pratiques et analyses réalisés pour le cours Big Data Analytics.

## 📂 Navigation
Utilisez le menu de gauche (ou l'explorateur) pour naviguer dans les différents laboratoires :

* **Lab 3** : PageRank & Spam Classification
* **Rapports** : Analyses détaillées

> **Auteur :** Elliot CAMBIER / Badr TAJINI
> **Dépôt GitHub :** [Lien vers le repo]($REPO_URL)
EOF

# 5. Construction finale
echo ">>> 🏗️ Construction du site..."
cd "$QUARTZ_DIR"
npx quartz build
cd ..

# 6. Préparation pour GitHub Pages
echo ">>> 💾 Sauvegarde dans 'docs/'..."
rm -rf docs
mv "$QUARTZ_DIR/public" docs
touch docs/.nojekyll

# Nettoyage final
rm -rf "$QUARTZ_DIR"

echo "✅ TERMINÉ ! Ton site complet est prêt dans le dossier 'docs/'."