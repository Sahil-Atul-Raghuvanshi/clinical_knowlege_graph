"""
Query Processor Module
Main orchestrator for the RAG pipeline
"""
import logging
from typing import Dict, Any, Tuple, Optional
from hybrid_retriever import HybridRetriever
from context_builder import ContextBuilder
from llm_module import LLMModule

logger = logging.getLogger(__name__)


class QueryProcessor:
    """Main query processing orchestrator"""
    
    def __init__(
        self,
        hybrid_retriever: Optional[HybridRetriever] = None,
        context_builder: Optional[ContextBuilder] = None,
        llm_module: Optional[LLMModule] = None
    ):
        """
        Initialize query processor
        
        Args:
            hybrid_retriever: Hybrid retriever instance
            context_builder: Context builder instance
            llm_module: LLM module instance
        """
        self.hybrid_retriever = hybrid_retriever or HybridRetriever()
        self.context_builder = context_builder or ContextBuilder()
        self.llm_module = llm_module
        
        if not self.llm_module:
            try:
                self.llm_module = LLMModule()
            except Exception as e:
                logger.warning(f"Could not initialize LLM module: {e}. LLM features will be disabled.")
                self.llm_module = None
        
        logger.info("QueryProcessor initialized")
    
    def close(self):
        """Close all connections"""
        if self.hybrid_retriever:
            self.hybrid_retriever.close()
    
    def process_query(
        self,
        query: str,
        top_k: int = 20,
        generate_answer: bool = True
    ) -> Dict[str, Any]:
        """
        Process a clinical query through the full RAG pipeline
        
        Args:
            query: User query text
            top_k: Number of results to retrieve
            generate_answer: Whether to generate LLM answer
            
        Returns:
            Dictionary containing:
                - answer: LLM-generated answer (if generate_answer=True)
                - context: Structured context
                - retrieval_results: Raw retrieval results
                - intent: Detected intent
        """
        logger.info(f"Processing query: {query[:100]}...")
        
        try:
            # Step 1: Hybrid retrieval
            retrieval_results = self.hybrid_retriever.retrieve(
                query=query,
                top_k=top_k
            )
            
            intent = retrieval_results.get("intent", "general")
            entities = retrieval_results.get("entities", {})
            merged_results = retrieval_results.get("merged_results", {})
            
            # Step 2: Build context
            context = self.context_builder.build_context(
                merged_results=merged_results,
                entities=entities,
                intent=intent
            )
            
            # Step 3: Generate answer (if LLM is available)
            answer = None
            if generate_answer and self.llm_module:
                patient_id = entities.get("patient_id")
                answer = self.llm_module.generate_response(
                    query=query,
                    context=context,
                    intent=intent,
                    patient_id=patient_id
                )
            elif generate_answer:
                answer = "LLM module is not available. Please check your Gemini API key configuration."
            
            return {
                "answer": answer,
                "context": context,
                "retrieval_results": retrieval_results,
                "intent": intent,
                "entities": entities
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {e}", exc_info=True)
            return {
                "answer": f"I encountered an error processing your query: {str(e)}",
                "context": {},
                "retrieval_results": {},
                "intent": "general",
                "entities": {},
                "error": str(e)
            }
    
    def process_query_streaming(
        self,
        query: str,
        top_k: int = 20
    ):
        """
        Process query with streaming response
        
        Args:
            query: User query
            top_k: Number of results to retrieve
            
        Yields:
            Response chunks
        """
        logger.info(f"Processing query with streaming: {query[:100]}...")
        
        try:
            # Step 1: Hybrid retrieval
            retrieval_results = self.hybrid_retriever.retrieve(
                query=query,
                top_k=top_k
            )
            
            intent = retrieval_results.get("intent", "general")
            entities = retrieval_results.get("entities", {})
            merged_results = retrieval_results.get("merged_results", {})
            
            # Step 2: Build context
            context = self.context_builder.build_context(
                merged_results=merged_results,
                entities=entities,
                intent=intent
            )
            
            # Step 3: Stream answer
            if self.llm_module:
                patient_id = entities.get("patient_id")
                for chunk in self.llm_module.generate_streaming_response(
                    query=query,
                    context=context,
                    intent=intent,
                    patient_id=patient_id
                ):
                    yield chunk
            else:
                yield "LLM module is not available. Please check your Gemini API key configuration."
                
        except Exception as e:
            logger.error(f"Error in streaming query processing: {e}")
            yield f"Error: {str(e)}"

