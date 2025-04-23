import pandas as pd
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_selection import RFECV
from catboost import CatBoostClassifier

def feature_elimination_model(
    df: pd.DataFrame,
    test_size: float = 0.2,
    random_state: int = 42
):
    """
    df: 'profit_pct' 컬럼 포함된 백테스트 결과 DataFrame
    test_size: 테스트셋 비율
    random_state: 재현성 시드

    반환:
        selector: 학습된 RFECV selector 객체
        label_encoder: profit_cat 레이블을 인코딩한 LabelEncoder
    """
    # 1) profit_pct 결측 제거 & 카테고리화
    df = df[df['profit_pct'].notnull()].copy()
    df['profit_cat'] = pd.qcut(
        df['profit_pct'],
        q=3,
        labels=['하', '중', '상']
    )

    # 2) 레이블 인코딩
    le = LabelEncoder()
    y = le.fit_transform(df['profit_cat'])

    # 3) 가능한 모든 수치형 피처 선택 (profit_pct 제외)
    X = df.select_dtypes(include='number').drop(columns=['profit_pct'])

    # 4) 학습/테스트 분할 (여기서는 RFECV에 전체 데이터를 사용하므로 분할은 선택적)
    #    StratifiedKFold 로 CV 전략 정의
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_state)

    # 5) CatBoost 분류기 정의
    model = CatBoostClassifier(
        iterations=300,
        learning_rate=0.1,
        depth=6,
        eval_metric='Accuracy',
        random_seed=random_state,
        verbose=0,
        allow_writing_files=False    # ← 임시 디렉터리 쓰기 비활성화
    )

    # 6) RFECV: 재귀적 피처 제거 + CV
    selector = RFECV(
        estimator=model,
        step=1,
        cv=cv,
        scoring='accuracy',
        n_jobs=-1,
        min_features_to_select=1
    )

    selector.fit(X, y)

    # 7) 결과 출력
    selected_feats = X.columns[selector.support_].tolist()
    feat_ranking = dict(zip(X.columns, selector.ranking_))

    print("▶︎ 최종 선택된 피처 (%d개):" % len(selected_feats))
    print(selected_feats)
    print("\n▶︎ 각 피처 랭킹 (1=선택됨, 숫자 클수록 중요도 낮음):")
    for feat, rank in sorted(feat_ranking.items(), key=lambda x: x[1]):
        print(f"  {feat}: {rank}")

    return selector, le

if __name__ == '__main__':
    # 예시: 백테스트 결과 불러오기
    df_result = pd.read_excel("results/results.xlsx")
    df_result.drop(['sell_price', 'sell_date', 'buy_date', 'duration', 'order'], axis=1, inplace=True)
    df_result = df_result[['buy_price', 'cor', 'vrate', 'mapct', 'order', '시가총액', 'days_since_max_high', 'PBR']]
    selector, label_encoder = feature_elimination_model(df_result)
