-- Lấy nguồn cấp từ output Bài 1 và bộ dữ liệu gốc
raw_data = LOAD '/Lab02_DS/hotel-review.csv' 
    USING PigStorage(';') 
    AS (
        id:chararray, 
        review:chararray, 
        category:chararray, 
        aspect:chararray, 
        sentiment:chararray
    );

words_final = LOAD '/Lab02_DS/output_bai1' 
    USING PigStorage(';') 
    AS (
        id:chararray, 
        word:chararray, 
        category:chararray, 
        aspect:chararray, 
        sentiment:chararray
    );

-- 2.1 Thống kê từ xuất hiện > 500 lần
word_group = GROUP words_final BY word;

word_count = FOREACH word_group 
    GENERATE 
        group AS word, 
        COUNT(words_final) AS total;

word_count_500 = FILTER word_count BY total > 500;


STORE word_count_500 INTO '/Lab02_DS/output_bai2_1' USING PigStorage(';');

-- 2.2 Thống kê số lượng bình luận theo category
cat_group = GROUP raw_data BY category;

cat_count = FOREACH cat_group 
    GENERATE 
        group AS category, 
        COUNT(raw_data) AS total;


STORE cat_count INTO '/Lab02_DS/output_bai2_2' USING PigStorage(';');

-- 2.3 Thống kê số lượng bình luận theo aspect
aspect_group = GROUP raw_data BY aspect;

aspect_count = FOREACH aspect_group 
    GENERATE 
        group AS aspect, 
        COUNT(raw_data) AS total;


STORE aspect_count INTO '/Lab02_DS/output_bai2_3' USING PigStorage(';');
