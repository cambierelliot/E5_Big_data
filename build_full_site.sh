#!/bin/bash
# Script de génération sélective (Seulement Assignment, pas Practice)

QUARTZ_DIR="quartz-min"
REPO_URL="https://github.com/cambierelliot/E5_Big_data"

echo "=== 🚀 Génération du Site (Mode Filtré) ==="

# 1. Installation propre
rm -rf "$QUARTZ_DIR"
echo ">>> 📦 Clonage de Quartz..."
git clone https://github.com/jackyzha0/quartz.git "$QUARTZ_DIR" > /dev/null 2>&1
cd "$QUARTZ_DIR" && npm ci > /dev/null 2>&1 && cd ..

# Dossiers de destination
CONTENT_DIR="$QUARTZ_DIR/content"
STATIC_NB_DIR="$CONTENT_DIR/static/nb"
mkdir -p "$STATIC_NB_DIR"

# ---------------------------------------------------------
# 2. TRAITEMENT DES NOTEBOOKS (.ipynb) -> HTML -> Iframe
# ---------------------------------------------------------
echo ">>> 📓 Traitement des Notebooks (Dossiers 'assignment' uniquement)..."

# On cherche uniquement dans les dossiers qui contiennent "assignment"
find . -type f -path "*assignment/*.ipynb" -not -path "*/.*" | while read notebook; do
    filename=$(basename "$notebook" .ipynb)
    parent_dir=$(dirname "$notebook")       # ex: ./lab3/assignment
    clean_dir=${parent_dir#./}              # ex: lab3/assignment
    
    echo "   ➡️  Notebook trouvé : $clean_dir/$filename"
    
    # A. Conversion en HTML (pour l'iframe)
    jupyter nbconvert --to html "$notebook" --output-dir "$STATIC_NB_DIR" --template basic --quiet
    
    # B. Création du dossier dans le site
    mkdir -p "$CONTENT_DIR/$clean_dir"
    
    # C. Calcul du chemin relatif pour remonter à la racine (ex: ../../)
    rel_path=$(echo "$clean_dir" | sed 's|[^/]\+|..|g')
    if [ -n "$rel_path" ]; then rel_path="$rel_path/"; fi
    
    # D. Création de la page Markdown Wrapper
    cat <<EOF > "$CONTENT_DIR/$clean_dir/$filename.md"
---
title: 📓 $filename
---
> [📄 Voir le fichier source sur GitHub]($REPO_URL/blob/main/$clean_dir/$filename.ipynb)

<iframe src="${rel_path}static/nb/$filename.html" width="100%" height="1200px" style="border:none;"></iframe>
EOF
done

# ---------------------------------------------------------
# 3. TRAITEMENT DES RAPPORTS (.md)
# ---------------------------------------------------------
echo ">>> 📝 Traitement des Rapports Markdown..."

# On cherche les .md dans "assignment" (excluant README et fichiers système)
find . -type f -path "*assignment/*.md" -not -name "README.md" | while read mdfile; do
    filename=$(basename "$mdfile" .md)
    parent_dir=$(dirname "$mdfile")
    clean_dir=${parent_dir#./}
    
    echo "   ➡️  Rapport trouvé : $clean_dir/$filename"
    
    mkdir -p "$CONTENT_DIR/$clean_dir"
    
    # On copie le fichier. Quartz va le traiter nativement.
    cp "$mdfile" "$CONTENT_DIR/$clean_dir/"
done

# 4. Page d'accueil (Index)
cat <<EOF > "$CONTENT_DIR/index.md"
---
title: Portfolio Big Data
---
# 📊 Big Data Analytics - Portfolio

Bienvenue. Ce site présente uniquement les **Assignments** (rendus notés).

## 📂 Accès rapide
Utilisez le menu de gauche pour naviguer.

* **Notebooks** : Le code exécuté et les résultats.
* **Rapports** : Les analyses textuelles et réponses aux questions.

> **Auteur :** Elliot CAMBIER / Badr TAJINI
EOF

# 5. Build & Deploy Prep
echo ">>> 🏗️  Construction du site..."
cd "$QUARTZ_DIR"
npx quartz build > /dev/null 2>&1
cd ..

echo ">>> 💾 Mise à jour du dossier 'docs'..."
rm -rf docs
mv "$QUARTZ_DIR/public" docs
touch docs/.nojekyll

# Nettoyage (Optionnel, désactive-le si tu veux debugger)
rm -rf "$QUARTZ_DIR"

echo "✅ TERMINÉ ! Prêt à pousser sur GitHub."