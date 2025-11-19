#!/bin/bash
# Script FINAL V2 : Correction Images, Lab2 et Iframes

QUARTZ_DIR="quartz-min"
REPO_USER="cambierelliot"
REPO_NAME="E5_Big_data"
BRANCH="main"

# URL pour les fichiers bruts (Images)
RAW_URL="https://raw.githubusercontent.com/$REPO_USER/$REPO_NAME/$BRANCH"
# URL pour les liens GitHub (Code source)
BLOB_URL="https://github.com/$REPO_USER/$REPO_NAME/blob/$BRANCH"

echo "=== 🚀 Génération du Site (Version Corrigée) ==="

# 1. Installation propre
rm -rf "$QUARTZ_DIR"
echo ">>> 📦 Clonage de Quartz..."
git clone https://github.com/jackyzha0/quartz.git "$QUARTZ_DIR" > /dev/null 2>&1
cd "$QUARTZ_DIR" && npm ci > /dev/null 2>&1 && cd ..

# Dossiers de destination
CONTENT_DIR="$QUARTZ_DIR/content"
# CORRECTION 1 : On met les HTML dans le dossier 'static' officiel de Quartz (pas content)
STATIC_NB_DIR="$QUARTZ_DIR/static/nb"
mkdir -p "$STATIC_NB_DIR"

# ---------------------------------------------------------
# 2. TRAITEMENT DES NOTEBOOKS
# ---------------------------------------------------------
echo ">>> 📓 Traitement des Notebooks..."

# CORRECTION 2 : '-iname' pour ignorer la casse et '*assign*' pour prendre singulier et pluriel
find . -type f -wholename "*assign*/*.ipynb" -not -path "*/.*" | while read notebook; do
    filename=$(basename "$notebook" .ipynb)
    parent_dir=$(dirname "$notebook")
    clean_dir=${parent_dir#./}
    
    echo "   ➡️  $clean_dir/$filename"
    
    # A. Conversion en HTML dans le dossier static
    jupyter nbconvert --to html "$notebook" --output-dir "$STATIC_NB_DIR" --template basic
    
    # B. Création du dossier dans le site
    mkdir -p "$CONTENT_DIR/$clean_dir"
    
    # C. Calcul du chemin relatif pour l'iframe
    # Compte le nombre de dossiers pour remonter à la racine (ex: lab1/assignment -> ../../)
    depth=$(echo "$clean_dir" | tr -cd '/' | wc -c)
    rel_prefix=""
    for ((i=0; i<=depth; i++)); do rel_prefix="../$rel_prefix"; done
    
    # D. Page Markdown Wrapper
    cat <<EOF > "$CONTENT_DIR/$clean_dir/$filename.md"
---
title: 📓 $filename
---
> [📄 Voir le fichier source sur GitHub]($BLOB_URL/$clean_dir/$filename.ipynb)

<iframe src="${rel_prefix}static/nb/$filename.html" width="100%" height="1200px" style="border:none;"></iframe>
EOF
done

# ---------------------------------------------------------
# 3. TRAITEMENT DES RAPPORTS (.md) + CORRECTION IMAGES
# ---------------------------------------------------------
echo ">>> 📝 Traitement des Rapports (Correction des images)..."

find . -type f -wholename "*assign*/*.md" -not -name "README.md" | while read mdfile; do
    filename=$(basename "$mdfile" .md)
    parent_dir=$(dirname "$mdfile")
    clean_dir=${parent_dir#./}
    
    echo "   ➡️  Rapport : $clean_dir/$filename"
    mkdir -p "$CONTENT_DIR/$clean_dir"
    
    dest_file="$CONTENT_DIR/$clean_dir/$filename.md"
    cp "$mdfile" "$dest_file"
    
    # CORRECTION 3 : Remplacement des liens d'images locaux par des liens GitHub Raw
    # Remplace "proof/..." par "https://raw.github.../proof/..."
    # Remplace "outputs/..." par "https://raw.github.../outputs/..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # Version Mac
        sed -i '' "s|proof/|$RAW_URL/proof/|g" "$dest_file"
        sed -i '' "s|outputs/|$RAW_URL/outputs/|g" "$dest_file"
    else
        # Version Linux/Windows (Git Bash/WSL)
        sed -i "s|proof/|$RAW_URL/proof/|g" "$dest_file"
        sed -i "s|outputs/|$RAW_URL/outputs/|g" "$dest_file"
    fi
done

# 4. Page d'accueil
cat <<EOF > "$CONTENT_DIR/index.md"
---
title: Portfolio Big Data
---
# 📊 Big Data Analytics - Portfolio

Bienvenue. Ce site présente les rendus des Assignments (Lab 1, 2 et 3).

## 📂 Accès rapide
Utilisez le menu de gauche pour naviguer.

* **Lab 1, 2, 3** : Retrouvez les Notebooks (Code) et les Rapports (Analyses).
* **Preuves** : Les images et plans d'exécution sont hébergés sur GitHub.

> **Auteur :** Elliot CAMBIER / Badr TAJINI
EOF

# 5. Build & Deploy
echo ">>> 🏗️  Construction du site..."
cd "$QUARTZ_DIR"
npx quartz build > /dev/null 2>&1
cd ..

echo ">>> 💾 Mise à jour du dossier 'docs'..."
rm -rf docs
mv "$QUARTZ_DIR/public" docs
touch docs/.nojekyll

# Nettoyage
rm -rf "$QUARTZ_DIR"

echo "✅ TERMINÉ ! Prêt à pousser sur GitHub."