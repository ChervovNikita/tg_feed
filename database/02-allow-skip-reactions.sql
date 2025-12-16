-- Allow reaction=0 for skip (previously only 1=like, -1=dislike)
-- This migration allows storing skipped posts in reactions table
-- so they can be excluded from future recommendations

-- Note: SMALLINT already allows 0, but the comment suggested only 1/-1
-- We're just updating the comment to reflect the new usage
COMMENT ON COLUMN reactions.reaction IS '1=like, -1=dislike, 0=skip';

