-- Setup dữ liệu gốc
raw_data = LOAD '/Lab02_DS/hotel-review.csv' 
    USING PigStorage(';') 
    AS (
        id:chararray, 
        review:chararray, 
        category:chararray, 
        aspect:chararray, 
        sentiment:chararray
    );

-- Khía cạnh có đánh giá Negative nhiều nhất
neg_data = FILTER raw_data BY sentiment == 'negative';

neg_group = GROUP neg_data BY aspect;

neg_count = FOREACH neg_group 
    GENERATE 
        group AS aspect, 
        COUNT(neg_data) AS total;

neg_ordered = ORDER neg_count BY total DESC;

STORE neg_ordered INTO '/Lab02_DS/output_bai3_1' USING PigStorage(';');

-- Khía cạnh có đánh giá Positive nhiều nhất
pos_data = FILTER raw_data BY sentiment == 'positive';

pos_group = GROUP pos_data BY aspect;

pos_count = FOREACH pos_group 
    GENERATE 
        group AS aspect, 
        COUNT(pos_data) AS total;

pos_ordered = ORDER pos_count BY total DESC;

STORE pos_ordered INTO '/Lab02_DS/output_bai3_2' USING PigStorage(';');
