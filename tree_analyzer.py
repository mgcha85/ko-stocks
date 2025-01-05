import pandas as pd
from common.utils import load_yaml
from catboost import CatBoostClassifier, Pool
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report


config = load_yaml('common/config.yaml')

## Step 1: load results
df = pd.concat(
    [pd.read_excel('results/KS.results.xlsx')]
)

# Step 2: Ensure all required features are present in the DataFrame
missing_features = [feat for feat in config['features'] if feat not in df.columns]
if missing_features:
    raise ValueError(f"Missing features in the data: {missing_features}")

# Step 3: set X, y 
X = df[config['features']]
y = (df['duration'] > 60).astype(int)

class_counts = y.value_counts()
class_weights = {0: 1.0, 1: class_counts[0] / class_counts[1]}

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Step 4: Train the CatBoost Model
# Initialize CatBoostRegressor; for classification, use CatBoostClassifier
model = CatBoostClassifier(
    iterations=1000,
    learning_rate=0.1,
    depth=6,
    loss_function='Logloss',
    class_weights=[class_weights[0], class_weights[1]],
    verbose=100
)
model.fit(X_train, y_train, eval_set=(X_test, y_test), early_stopping_rounds=50)

# Step 5: Evaluate the Model
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
report = classification_report(y_test, y_pred)

print(f"Accuracy: {accuracy}")
print("Classification Report:")
print(report)
