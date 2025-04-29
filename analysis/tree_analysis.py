import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from catboost import CatBoostClassifier, Pool

def train_profit_category_model(df, feature_cols=None, test_size=0.2, random_state=42):
    """
    df: profit_pct를 포함한 DataFrame
    feature_cols: 사용할 피처 리스트. 기본 None이면 자동으로 선택.
    test_size: 테스트셋 비율
    random_state: 재현성 시드
    """

    # 1) profit_pct를 3분위로 나눠서 범주화
    df = df.copy()
    df = df[df['profit_pct'].notnull()]  # 결측 제거
    df['profit_cat'] = pd.cut(
        df['duration'],
        bins=3,
        labels=[0, 1, 2]
    )

    # 2) 피처 선택
    if feature_cols is None:
        # 수치형 컬럼 중 target 빼고 전부 사용
        feature_cols = [
            col for col in df.select_dtypes(include='number').columns
            if col not in ['profit_pct']
        ]

    X = df[feature_cols]
    y = df['profit_cat']

    # 3) 학습/테스트 분할
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=test_size,
        stratify=y,
        random_state=random_state
    )

    # 4) CatBoostClassifier 학습
    model = CatBoostClassifier(
        iterations=500,
        learning_rate=0.1,
        depth=6,
        eval_metric='Accuracy',
        random_seed=random_state,
        verbose=100
    )
    train_pool = Pool(X_train, y_train)
    model.fit(train_pool, use_best_model=True)

    # 5) 평가
    y_pred = model.predict(X_test)
    print("=== Confusion Matrix ===")
    print(confusion_matrix(y_test, y_pred))
    print("\n=== Classification Report ===")
    print(classification_report(y_test, y_pred, digits=4))

    model.save_model("models/profit_category_model.cbm")

    return model

if __name__ == '__main__':
    # 예: 백테스트 결과 Excel을 읽어와 모델 학습
    df_result = pd.read_excel("results/results.xlsx")
    
    # days_since_max_high, kospi_index 같은 추가 피처가 있다 가정
    # cor, vrate, mapct, days_since_max_high, kospi_index 등
    features = ['buy_price', 'cor', 'vrate', 'mapct', '거래대금', '시가총액', 'days_since_max_high', 'BPS', 'PER', 'PBR', 'DIV']

    model = train_profit_category_model(
        df_result,
        feature_cols=features,
        test_size=0.25
    )
