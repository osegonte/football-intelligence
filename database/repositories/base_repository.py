# database/repositories/base_repository.py
class BaseRepository:
    def __init__(self, session):
        self.session = session
        
    def add(self, entity):
        self.session.add(entity)
        return entity
        
    def add_all(self, entities):
        self.session.add_all(entities)
        return entities
        
    def delete(self, entity):
        self.session.delete(entity)
        
    def commit(self):
        self.session.commit()
        
    def rollback(self):
        self.session.rollback()