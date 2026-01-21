-- View open items
SELECT review_id, left_vendor_id, right_vendor_id, confidence, recommendation, rationale
FROM mdm.vendor_review_queue
WHERE status='OPEN'
ORDER BY confidence DESC;

-- Approve a merge (example)
UPDATE mdm.vendor_review_queue
SET status='APPROVED', reviewed_at=NOW(), reviewed_by='data_steward', decision_notes='Approved merge'
WHERE review_id = 123;

-- Reject a merge
UPDATE mdm.vendor_review_queue
SET status='REJECTED', reviewed_at=NOW(), reviewed_by='data_steward', decision_notes='Not same vendor'
WHERE review_id = 124;
