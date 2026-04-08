-- 1. Load data
raw_data = LOAD '/Lab02_DS/hotel-review.csv' 
    USING PigStorage(';') 
    AS (
        id:chararray, 
        review:chararray, 
        category:chararray, 
        aspect:chararray, 
        sentiment:chararray
    );

-- 2. Đưa tất cả ký tự về chữ thường (lowercase)
data_lower = FOREACH raw_data 
    GENERATE 
        id, 
        LOWER(review) AS review, 
        category, 
        aspect, 
        sentiment;

-- 3. Tách các dòng bình luận thành dãy từ
words = FOREACH data_lower 
    GENERATE 
        id, 
        FLATTEN(TOKENIZE(review, ' ')) AS word, 
        category, 
        aspect, 
        sentiment;

-- 4. Loại bỏ các stop word
stopwords = LOAD '/Lab02_DS/stopwords.txt' 
    USING PigStorage() 
    AS (word:chararray);

joined_words = JOIN words BY word LEFT OUTER, stopwords BY word;

words_filtered = FILTER joined_words BY stopwords::word IS NULL;

words_final = FOREACH words_filtered 
    GENERATE 
        words::id AS id, 
        words::word AS word, 
        words::category AS category, 
        words::aspect AS aspect, 
        words::sentiment AS sentiment;

-- Ghi kết quả ra DUMP và lưu lại để dùng cho Bài 2, 4, 5
DUMP words_final;

STORE words_final 
    INTO '/Lab02_DS/output_bai1' 
    USING PigStorage(';');
