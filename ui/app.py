import streamlit as st
import hopsworks
import joblib
import pandas as pd
import numpy as np

# --- 1. SETUP & CONNECTION ---
st.title("⭐ GitHub Release Star Predictor")
st.write("Enter release details to predict popularity.")

@st.cache_resource
def load_model_from_hopsworks():
    project = hopsworks.login() 
    mr = project.get_model_registry()
    
    try:
        # Update version if needed (e.g., version=1 or version=2)
        model_meta = mr.get_model("github_star_predictor_XGB", version=2)
    except:
        st.error("Model not found in Hopsworks Registry.")
        st.stop()
    
    saved_path = model_meta.download()
    artifacts = joblib.load(f"{saved_path}/model_XGB_artifacts.pkl")
    return artifacts

try:
    artifacts = load_model_from_hopsworks()
    model = artifacts['model']
    tfidf_body = artifacts['tfidf_body']
    tfidf_name = artifacts['tfidf_name']
    st.success("Model loaded successfully!")
except Exception as e:
    st.error(f"Failed to load model: {e}")
    st.stop()

# --- 2. USER INPUTS ---
col1, col2 = st.columns(2)

with col1:
    # Feature: repo_duration (mapped from 'repo_age_days')
    repo_age = st.number_input("Repo Age (Days)", min_value=0, value=365)
    
    # Feature: author_followers
    followers = st.number_input("Author Followers", min_value=0, value=100)
    
    # Feature: author_public_repos
    public_repos = st.number_input("Author Public Repos", min_value=0, value=20)
    
    # Feature: Language (One-Hot Encoded logic)
    # List derived from your error message
    language_options = [
        'c', 'csharp', 'cplusplus', 'css', 'dart', 'go', 'html', 'java', 
        'javascript', 'kotlin', 'lua', 'other', 'php', 'python', 'qml', 
        'rust', 'shell', 'svelte', 'swift', 'typescript', 'vue'
    ]
    selected_language = st.selectbox("Repository Language", language_options, index=13) # Default Python

with col2:
    release_name = st.text_input("Release Name", value="v1.0.0 Major Update")
    release_body = st.text_area("Release Notes", value="Fix bugs and add new AI features.")

# Hidden/Default Features
is_weekend = False # Default assumption
author_type = "User" # Default assumption

# --- 3. PRE-PROCESSING ---
if st.button("Predict Stars"):
    
    # A. Text Processing (TF-IDF)
    body_mat = tfidf_body.transform([release_body])
    # IMPORTANT: Use the exact names from vectorizer, assuming they match model training
    body_df = pd.DataFrame(body_mat.toarray(), columns=[f"body_{k}" for k in tfidf_body.get_feature_names_out()])
    
    name_mat = tfidf_name.transform([release_name])
    name_df = pd.DataFrame(name_mat.toarray(), columns=[f"name_{k}" for k in tfidf_name.get_feature_names_out()])
    
    # B. Construct the Main DataFrame
    # We initialize a dictionary with 0s for ALL expected columns from your error message
    # This ensures we never miss a column.
    
    expected_cols = [
        'author_followers', 'author_public_repos', 'repo_duration', 
        'org_author', 'user_author', 'publish_is_weekday', 
        'is_weekend' 
    ]
    
    # Add language columns dynamically
    lang_cols = [f"language__{l}" for l in language_options]
    
    # Create the base input with 0s
    input_data = pd.DataFrame(0, index=[0], columns=expected_cols + lang_cols)
    
    # C. Fill Values
    input_data['author_followers'] = followers
    input_data['author_public_repos'] = public_repos
    input_data['repo_duration'] = repo_age
    
    # Handle Date Booleans
    input_data['is_weekend'] = 1 if is_weekend else 0
    input_data['publish_is_weekday'] = 0 if is_weekend else 1
    
    # Handle Author Type
    if author_type == "User":
        input_data['user_author'] = 1
        input_data['org_author'] = 0
    else:
        input_data['user_author'] = 0
        input_data['org_author'] = 1
        
    # Handle Language (The Key Fix)
    # We set the selected language column to 1, others remain 0
    selected_col = f"language__{selected_language}"
    if selected_col in input_data.columns:
        input_data[selected_col] = 1
    else:
        # Fallback if somehow selected language isn't in training set
        if 'language__other' in input_data.columns:
            input_data['language__other'] = 1

    # D. Combine Everything
    # We concat the main data with the text vectors
    final_input = pd.concat([input_data, body_df, name_df], axis=1)
    
    # E. Final alignment check
    # The model might have trained on specific column order or extra text columns
    # We assume 'final_input' covers most. If there are still missing columns (e.g. text keywords not in current input),
    # we need to add them as 0.
    
    # (Optional) Re-order to match model.feature_names_in_ if available
    if hasattr(model, "feature_names_in_"):
        # Add missing columns with 0
        for col in model.feature_names_in_:
            if col not in final_input.columns:
                final_input[col] = 0
        # Select and reorder
        final_input = final_input[model.feature_names_in_]
    
    try:
        prediction = model.predict(final_input)[0]
        prediction = max(0, prediction) # Clip negative
        st.metric(label="Predicted Stars (First Week)", value=f"{int(prediction)}")
    except Exception as e:
        st.error(f"Prediction Error: {e}")
        st.write("Columns sent to model:", final_input.columns.tolist())