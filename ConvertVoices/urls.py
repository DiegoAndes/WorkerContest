from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^convertirVoces/$', views.convertVoices, name='convertirVoces')
]
