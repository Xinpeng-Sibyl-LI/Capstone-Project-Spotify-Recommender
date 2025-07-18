import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
from xgboost import XGBClassifier
import pickle
import os
from pathlib import Path

def main():
    # === Load Data ===
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    # Go up one level to project root, then into data folder
    data_path = script_dir.parent / "data" / "raw_tracks.csv"
    
    tracks = pd.read_csv(data_path)
    
    # Check what columns are available
    print("Available columns in the dataset:")
    print(tracks.columns.tolist())
    print(f"\nDataset shape: {tracks.shape}")
    
    # Clean data - using correct column names
    tracks = tracks.dropna(subset=['popularity', 'duration_ms'])

    # === Binary Target ===
    # Using 'popularity' instead of 'track_popularity'
    tracks['is_popular'] = tracks['popularity'].apply(lambda x: 1 if x >= 85 else 0)

    # === Feature Engineering ===
    tracks['duration_sec'] = tracks['duration_ms'] / 1000
    # Using 'name' instead of 'track_name'
    tracks['track_name_length'] = tracks['name'].apply(lambda x: len(str(x)))
    
    # For album name, we need to extract it from the complex 'album' column
    # Let's try a simple approach first - you might need to adjust this
    tracks['album_name_length'] = tracks['album'].apply(lambda x: len(str(x)))
    
    # Using 'name' instead of 'track_name' for remix detection
    tracks['has_remix'] = tracks['name'].str.lower().str.contains("remix", na=False).astype(int)
    
    # Using 'explicit' instead of 'is_explicit'
    tracks['interaction'] = tracks['duration_sec'] * tracks['explicit'].astype(int)

    # === Features and Target ===
    features = ['duration_sec', 'track_name_length', 'album_name_length', 'explicit', 'has_remix', 'interaction']
    X = tracks[features]
    y = tracks['is_popular']

    # === Feature Scaling ===
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # === Train-Test Split ===
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

    # === Calculate Class Imbalance Ratio ===
    pos_ratio = (y == 0).sum() / (y == 1).sum()

    # === Train XGBoost Model ===
    xgb_model = XGBClassifier(
        objective='binary:logistic',
        eval_metric='logloss',
        use_label_encoder=False,
        scale_pos_weight=pos_ratio,
        max_depth=4,
        n_estimators=100,
        learning_rate=0.1,
        random_state=42
    )
    xgb_model.fit(X_train, y_train)

    # === Evaluation ===
    y_pred = xgb_model.predict(X_test)
    print("✅ Accuracy:", accuracy_score(y_test, y_pred))
    print("\nConfusion Matrix:\n", confusion_matrix(y_test, y_pred))
    print("\nClassification Report:\n", classification_report(y_test, y_pred))

    # === Feature Importance ===
    importances = xgb_model.feature_importances_
    feature_df = pd.DataFrame({'Feature': features, 'Importance': importances}).sort_values(by='Importance', ascending=False)

    plt.figure(figsize=(8, 6))
    sns.barplot(data=feature_df, x='Importance', y='Feature', palette='crest')
    plt.title("🔥 Feature Importance (XGBoost)")
    plt.tight_layout()
    plt.show()

    # === Save Model ===
    with open("xgb_tracks_model.pkl", "wb") as f:
        pickle.dump(xgb_model, f)

    print("✅ XGBoost model saved.")

if __name__ == "__main__":
    main()