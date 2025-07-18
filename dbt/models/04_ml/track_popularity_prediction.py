import pandas as pd
import numpy as np

def model(dbt, session):
    # Load data directly from raw table
    tracks_df = session.table("RAW_TOP_TRACKS").to_pandas()
    
    # Clean data
    tracks_df = tracks_df.dropna(subset=['POPULARITY', 'DURATION_MS'])
    
    # Create target variable (popular = popularity >= 85)
    tracks_df['IS_POPULAR'] = (tracks_df['POPULARITY'] >= 85).astype(int)
    
    # Feature engineering
    tracks_df['DURATION_SEC'] = tracks_df['DURATION_MS'] / 1000
    tracks_df['TRACK_NAME_LENGTH'] = tracks_df['NAME'].fillna('').astype(str).apply(len)
    tracks_df['HAS_REMIX'] = tracks_df['NAME'].fillna('').str.lower().str.contains('remix').astype(int)
    tracks_df['HAS_FEAT'] = tracks_df['NAME'].fillna('').str.lower().str.contains('feat').astype(int)
    tracks_df['EXPLICIT_NUM'] = tracks_df['EXPLICIT'].astype(int)
    
    # Rule-based prediction model
    def predict_popularity(row):
        score = 0
        
        # Duration preference (3-4 minutes tends to be popular)
        if 180 <= row['DURATION_SEC'] <= 240:
            score += 2
        elif 150 <= row['DURATION_SEC'] <= 300:
            score += 1
        elif row['DURATION_SEC'] > 360:  # Very long tracks less popular
            score -= 1
        
        # Shorter track names tend to be more memorable
        if row['TRACK_NAME_LENGTH'] < 20:
            score += 1
        elif row['TRACK_NAME_LENGTH'] > 50:
            score -= 1
        
        # Explicit content factor
        if row['EXPLICIT_NUM'] == 1:
            score += 0.5  # Slight boost but not major
        
        # Remix tracks tend to be less popular than originals
        if row['HAS_REMIX'] == 1:
            score -= 1
        
        # Featured tracks can be popular due to collaboration
        if row['HAS_FEAT'] == 1:
            score += 1
        
        # Convert score to probability (normalize to 0-1 range)
        # Assuming score range is roughly -2 to 5
        probability = max(0, min(1, (score + 2) / 7))
        
        # Predict 1 if probability > 0.6 (stricter threshold)
        prediction = 1 if probability > 0.6 else 0
        
        return prediction, probability
    
    # Apply predictions
    predictions = tracks_df.apply(
        lambda row: predict_popularity(row), 
        axis=1, 
        result_type='expand'
    )
    
    tracks_df['PREDICTED_POPULAR'] = predictions[0]
    tracks_df['PREDICTION_PROBABILITY'] = predictions[1]
    
    # Calculate accuracy
    correct_predictions = (tracks_df['IS_POPULAR'] == tracks_df['PREDICTED_POPULAR']).sum()
    total_predictions = len(tracks_df)
    accuracy = correct_predictions / total_predictions if total_predictions > 0 else 0
    
    # Create results
    results_df = pd.DataFrame({
        'TRACK_ID': tracks_df['ID'],
        'TRACK_NAME': tracks_df['NAME'],
        'ACTUAL_POPULARITY': tracks_df['POPULARITY'],
        'IS_ACTUALLY_POPULAR': tracks_df['IS_POPULAR'],
        'PREDICTED_POPULAR': tracks_df['PREDICTED_POPULAR'],
        'PREDICTION_PROBABILITY': tracks_df['PREDICTION_PROBABILITY'],
        'MODEL_ACCURACY': accuracy,
        'MODEL_TYPE': 'RULE_BASED',
        'TRAINED_AT': pd.Timestamp.now()
    })
    
    return results_df