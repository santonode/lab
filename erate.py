# === ADD TO POST BLOCK IN import_interactive() ===
if action == 'import_all':
    try:
        remaining = progress['total'] - progress['index'] + 1
        if remaining <= 0:
            return "No records left to import."

        imported = 0
        skipped = 0

        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [name.strip().lstrip('\ufeff') for name in reader.fieldnames]

            # Skip to current index
            for _ in range(progress['index'] - 1):
                next(reader)

            for row in reader:
                app_number = row.get('Application Number', '').strip()
                if not app_number:
                    skipped += 1
                    continue
                if db.session.get(Erate, app_number):
                    skipped += 1
                    continue

                erate = Erate(
                    app_number=app_number,
                    # ... all fields (same as single import)
                    last_modified_datetime=safe_date(row.get('Last Modified Date/Time')),
                    # ... etc
                )
                db.session.add(erate)
                imported += 1

                if imported % 100 == 0:
                    db.session.commit()  # Commit in batches

            db.session.commit()

        progress['success'] += imported
        progress['error'] += skipped
        progress['index'] = progress['total'] + 1
        session['import_progress'] = progress

        return f"""
        <h1>BULK IMPORT COMPLETE!</h1>
        <p>Imported: <strong>{imported}</strong> | Skipped: <strong>{skipped}</strong></p>
        <a href="/erate">Go to Dashboard</a>
        """

    except Exception as e:
        db.session.rollback()
        return f"Bulk import failed: {e}"
