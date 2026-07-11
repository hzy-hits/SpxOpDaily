ALTER TABLE compaction_manifests
    ADD COLUMN dataset TEXT NOT NULL DEFAULT 'quotes';
