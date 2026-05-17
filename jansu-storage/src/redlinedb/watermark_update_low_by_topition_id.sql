update watermark

set

low = case when $2 > low then $2 else low end

where topition = $1;
