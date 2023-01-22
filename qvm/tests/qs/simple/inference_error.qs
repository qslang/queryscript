-- This module fails to compile (it references a field that does not exist).
-- It imports the inference module too.
import error;

-- If we re-import inference, we should be able to access the inferred types.
import inference;

SELECT COUNT(*) FROM inference.events;
SELECT COUNT(*) FROM inference.users;
