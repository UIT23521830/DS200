-- Lấy nguồn cấp đã được qua tiền xử lý tách từ ở Bài 1
words_final = LOAD '/Lab02_DS/output_bai1' 
    USING PigStorage(';') 
    AS (
        id:chararray, 
        word:chararray, 
        category:chararray, 
        aspect:chararray, 
        sentiment:chararray
    );

-- 5 từ tích cực nhất mỗi category
pos_words = FILTER words_final BY sentiment == 'positive';

pos_cw_group = GROUP pos_words BY (category, word);

pos_cw_count = FOREACH pos_cw_group 
    GENERATE 
        FLATTEN(group) AS (category, word), 
        COUNT(pos_words) AS total;

cat_pos_grp = GROUP pos_cw_count BY category;

top5_pos = FOREACH cat_pos_grp {
    sorted = ORDER pos_cw_count BY total DESC;
    GENERATE 
        group AS category, 
        sorted;
}


STORE top5_pos INTO '/Lab02_DS/output_bai4_1' USING PigStorage(';');

-- 5 từ tiêu cực nhất mỗi category
neg_words = FILTER words_final BY sentiment == 'negative';

neg_cw_group = GROUP neg_words BY (category, word);

neg_cw_count = FOREACH neg_cw_group 
    GENERATE 
        FLATTEN(group) AS (category, word), 
        COUNT(neg_words) AS total;

cat_neg_grp = GROUP neg_cw_count BY category;

top5_neg = FOREACH cat_neg_grp {
    sorted = ORDER neg_cw_count BY total DESC;
    GENERATE 
        group AS category, 
        sorted;
}


STORE top5_neg INTO '/Lab02_DS/output_bai4_2' USING PigStorage(';');
