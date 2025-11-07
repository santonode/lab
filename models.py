# models.py
from app import db
from datetime import datetime

class Erate(db.Model):
    __tablename__ = 'erate'
    id = db.Column(db.String(50), primary_key=True)
    state = db.Column(db.String(2))
    funding_year = db.Column(db.String(4))
    entity_name = db.Column(db.Text)
    address = db.Column(db.Text)
    zip_code = db.Column(db.String(10))
    frn = db.Column(db.String(20))
    app_number = db.Column(db.String(20))
    status = db.Column(db.String(50))
    amount = db.Column(db.Float)
    description = db.Column(db.Text)
    last_modified = db.Column(db.DateTime)

    def __repr__(self):
        return f"<Erate {self.frn}>"
