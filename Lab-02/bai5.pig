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

cw_group = GROUP words_final BY (category, word);

cw_count = FOREACH cw_group 
    GENERATE 
        FLATTEN(group) AS (category, word), 
        COUNT(words_final) AS total;

cat_grp = GROUP cw_count BY category;

top5_relevant = FOREACH cat_grp {
    sorted = ORDER cw_count BY total DESC;
    GENERATE 
        group AS category, 
        sorted;
}


STORE top5_relevant INTO '/Lab02_DS/output_bai5' USING PigStorage(';');
