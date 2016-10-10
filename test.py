import ClimateImpactLab as lab
api = lab.Client()
api.refresh_database()
api.commit()