from django.urls import path
from . import views
from .views import ChatbotView

urlpatterns = [
    
   path('chat/', ChatbotView.as_view(), name='chatbot'),
]