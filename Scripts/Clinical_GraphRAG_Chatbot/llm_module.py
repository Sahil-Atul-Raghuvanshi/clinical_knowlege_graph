"""
LLM Module
Integrates with Gemini API for answer generation
"""
import logging
import os
from typing import Dict, Any, Optional
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class LLMModule:
    """Handles LLM interactions using Google Gemini"""
    
    def __init__(self, api_key: Optional[str] = None, model_name: Optional[str] = None):
        """
        Initialize LLM module
        
        Args:
            api_key: Gemini API key (uses GEMINI_API_KEY env var if not provided)
            model_name: Gemini model name (defaults to GEMINI_MODEL env var or "gemini-2.5-pro")
        """
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Gemini API key not found. Please set GEMINI_API_KEY environment variable "
                "or provide it in the constructor."
            )
        
        # Get model name from parameter, env var, or use default
        self.model_name = model_name or os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(self.model_name)
        
        logger.info(f"LLMModule initialized with model: {self.model_name}")
    
    def load_prompt_template(self, intent: str) -> str:
        """
        Load prompt template for specific intent
        
        Args:
            intent: Query intent (patient_similarity, treatment_recommendation, summary, general)
            
        Returns:
            Prompt template string
        """
        template_file = Path(__file__).parent / "prompts" / f"{intent}.txt"
        
        if not template_file.exists():
            # Fallback to general template
            template_file = Path(__file__).parent / "prompts" / "general.txt"
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error loading prompt template: {e}")
            return self._get_default_prompt()
    
    def _get_default_prompt(self) -> str:
        """Get default prompt template"""
        return """You are a clinical assistant using a knowledge graph to answer questions.

Context:
{context}

Question:
{question}

Please provide a clear, evidence-based answer using the provided context."""

    def generate_response(
        self,
        query: str,
        context: Dict[str, Any],
        intent: str = "general",
        patient_id: Optional[str] = None
    ) -> str:
        """
        Generate response using Gemini
        
        Args:
            query: User query
            context: Structured context from retrieval
            intent: Query intent
            patient_id: Optional patient ID for prompt formatting
            
        Returns:
            Generated response text
        """
        try:
            # Load appropriate prompt template
            template = self.load_prompt_template(intent)
            
            # Format context for prompt
            # Import here to avoid circular dependency
            try:
                from context_builder import ContextBuilder
                context_builder = ContextBuilder()
                context_text = context_builder.format_context_for_llm(context, patient_id)
            except ImportError:
                # Fallback: use JSON format
                import json
                context_text = json.dumps(context, indent=2, default=str)
            
            # Format prompt
            if intent == "patient_similarity" and patient_id:
                prompt = template.format(
                    context=context_text,
                    patient_id=patient_id
                )
            elif intent == "treatment_recommendation":
                condition = context.get("extracted_entities", {}).get("condition", "the condition")
                prompt = template.format(
                    context=context_text,
                    condition=condition
                )
            elif intent == "clinical_summary" and patient_id:
                prompt = template.format(
                    context=context_text,
                    patient_id=patient_id
                )
            else:
                prompt = template.format(
                    context=context_text,
                    question=query
                )
            
            # Generate response
            logger.info(f"Generating response with Gemini ({self.model_name})...")
            response = self.model.generate_content(prompt)
            
            # Extract text from response
            if hasattr(response, 'text'):
                answer = response.text
            elif hasattr(response, 'candidates') and response.candidates:
                answer = response.candidates[0].content.parts[0].text
            else:
                answer = "I apologize, but I couldn't generate a response. Please try rephrasing your question."
            
            logger.info("Response generated successfully")
            return answer
            
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            return f"I encountered an error while generating a response: {str(e)}. Please try again."
    
    def generate_streaming_response(
        self,
        query: str,
        context: Dict[str, Any],
        intent: str = "general",
        patient_id: Optional[str] = None
    ):
        """
        Generate streaming response (for real-time display)
        
        Args:
            query: User query
            context: Structured context
            intent: Query intent
            patient_id: Optional patient ID
            
        Yields:
            Response chunks
        """
        try:
            # Load prompt template
            template = self.load_prompt_template(intent)
            
            # Format context
            try:
                from context_builder import ContextBuilder
                context_builder = ContextBuilder()
                context_text = context_builder.format_context_for_llm(context, patient_id)
            except ImportError:
                # Fallback: use JSON format
                import json
                context_text = json.dumps(context, indent=2, default=str)
            
            # Format prompt
            if intent == "patient_similarity" and patient_id:
                prompt = template.format(
                    context=context_text,
                    patient_id=patient_id
                )
            elif intent == "treatment_recommendation":
                condition = context.get("extracted_entities", {}).get("condition", "the condition")
                prompt = template.format(
                    context=context_text,
                    condition=condition
                )
            elif intent == "clinical_summary" and patient_id:
                prompt = template.format(
                    context=context_text,
                    patient_id=patient_id
                )
            else:
                prompt = template.format(
                    context=context_text,
                    question=query
                )
            
            # Generate streaming response
            response = self.model.generate_content(
                prompt,
                stream=True
            )
            
            for chunk in response:
                if hasattr(chunk, 'text'):
                    yield chunk.text
                elif hasattr(chunk, 'candidates') and chunk.candidates:
                    yield chunk.candidates[0].content.parts[0].text
                    
        except Exception as e:
            logger.error(f"Error in streaming response: {e}")
            yield f"Error: {str(e)}"

