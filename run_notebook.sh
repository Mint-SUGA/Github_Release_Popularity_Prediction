cd /Users/cuiyuting/Desktop/Github_Release_Popularity_Prediction

export HOPSWORKS_API_KEY=""
export GITHUB_TOKEN=""

if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
fi

jupyter nbconvert --to notebook --execute daily_feature_pipeline.ipynb --output executed_notebook.ipynb

echo "$(date): Notebook executed" >> execution_log.txt