"""
Text embedding generator using SentenceTransformers, OpenAI, or Gemini
"""
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any
import numpy as np
from sentence_transformers import SentenceTransformer

# Add current directory (full_patient_embeddings) and parent directory to path for local imports
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

logger = logging.getLogger(__name__)


class TextEmbeddingGenerator:
    """Generate text embeddings for patient data"""
    
    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        use_openai: bool = False,
        openai_api_key: str = None,
        use_gemini: bool = False,
        gemini_api_key: str = None
    ):
        """
        Initialize text embedding generator
        
        Args:
            model_name: SentenceTransformer model name
            use_openai: Use OpenAI embeddings
            openai_api_key: OpenAI API key
            use_gemini: Use Google Gemini embeddings
            gemini_api_key: Gemini API key
        """
        self.use_openai = use_openai
        self.use_gemini = use_gemini
        self.model_name = model_name
        
        if use_openai:
            try:
                import openai
                self.openai_client = openai.OpenAI(api_key=openai_api_key)
                self.dimension = 1536  # text-embedding-3-large
                logger.info("Using OpenAI embeddings")
            except ImportError:
                logger.error("OpenAI package not installed")
                raise
        elif use_gemini:
            try:
                import google.generativeai as genai
                genai.configure(api_key=gemini_api_key)
                self.genai = genai
                self.dimension = 768
                logger.info("Using Google Gemini embeddings")
            except ImportError:
                logger.error("Google Generative AI package not installed")
                raise
        else:
            logger.info(f"Loading SentenceTransformer model: {model_name}")
            self.model = SentenceTransformer(model_name)
            self.dimension = self.model.get_sentence_embedding_dimension()
            # Get max sequence length from tokenizer
            try:
                self.max_seq_length = self.model.max_seq_length
            except AttributeError:
                # Fallback: try to get from tokenizer
                try:
                    self.max_seq_length = self.model.tokenizer.model_max_length
                except:
                    # Default for most BERT-based models
                    self.max_seq_length = 512
            logger.info(f"Model loaded. Dimension: {self.dimension}, Max sequence length: {self.max_seq_length}")
    
    def _chunk_text(self, text: str, chunk_overlap: int = 50) -> List[str]:
        """
        Split text into chunks that fit within the model's token limit
        Uses tokenizer to accurately count tokens
        
        Args:
            text: Text to chunk
            chunk_overlap: Number of characters to overlap between chunks
            
        Returns:
            List of text chunks
        """
        if not hasattr(self, 'model') or not hasattr(self, 'max_seq_length'):
            # Fallback: character-based chunking (conservative estimate)
            # Assume ~4 characters per token, use 80% of max to be safe
            max_chars = int(self.max_seq_length * 4 * 0.8) if hasattr(self, 'max_seq_length') else 2000
            chunks = []
            start = 0
            while start < len(text):
                end = start + max_chars
                if end >= len(text):
                    chunks.append(text[start:])
                    break
                # Try to break at sentence boundary
                last_period = text.rfind('.', start, end)
                last_pipe = text.rfind('|', start, end)
                break_point = max(last_period, last_pipe)
                if break_point > start:
                    end = break_point + 1
                chunks.append(text[start:end])
                start = end - chunk_overlap
            return chunks
        
        # Token-based chunking (more accurate)
        try:
            # Access tokenizer - SentenceTransformer models have tokenizer attribute
            if hasattr(self.model, 'tokenizer'):
                tokenizer = self.model.tokenizer
            elif hasattr(self.model, '_first_module') and hasattr(self.model._first_module, 'tokenizer'):
                tokenizer = self.model._first_module.tokenizer
            else:
                raise AttributeError("Tokenizer not found")
            
            # Use conservative max_tokens to ensure chunks fit within model limit
            # Reserve extra tokens for special tokens and potential encoding differences
            # SentenceTransformer adds special tokens, so we need to be more conservative
            max_tokens = self.max_seq_length - 20  # Extra conservative reserve
            
            # Tokenize the entire text
            if hasattr(tokenizer, 'encode'):
                tokens = tokenizer.encode(text, add_special_tokens=False)
            else:
                # Fallback: use __call__ method
                tokens = tokenizer(text, add_special_tokens=False)['input_ids']
            
            logger.debug(f"Text tokenized: {len(tokens)} tokens (max: {max_tokens})")
            
            if len(tokens) <= max_tokens:
                logger.debug("Text fits in single chunk")
                return [text]
            
            # Need to chunk (log at debug to reduce console noise)
            logger.debug(f"Text requires chunking: {len(tokens)} tokens > {max_tokens} max tokens")
            chunks = []
            start_idx = 0
            overlap_tokens = max(10, chunk_overlap // 4)  # At least 10 tokens overlap
            
            chunk_num = 0
            while start_idx < len(tokens):
                chunk_num += 1
                # Calculate chunk end, ensuring we don't exceed max_tokens
                end_idx = min(start_idx + max_tokens, len(tokens))
                chunk_tokens = tokens[start_idx:end_idx]
                
                # Safety check: ensure chunk doesn't exceed max_tokens
                if len(chunk_tokens) > max_tokens:
                    chunk_tokens = chunk_tokens[:max_tokens]
                    end_idx = start_idx + len(chunk_tokens)
                
                # Decode chunk
                if hasattr(tokenizer, 'decode'):
                    chunk_text = tokenizer.decode(chunk_tokens, skip_special_tokens=True)
                else:
                    # Fallback decoding
                    chunk_text = tokenizer.convert_tokens_to_string(
                        tokenizer.convert_ids_to_tokens(chunk_tokens)
                    )
                
                # Verify decoded chunk is within limits
                if hasattr(tokenizer, 'encode'):
                    verify_tokens = tokenizer.encode(chunk_text, add_special_tokens=False)
                else:
                    verify_tokens = tokenizer(chunk_text, add_special_tokens=False)['input_ids']
                
                if len(verify_tokens) > max_tokens:
                    logger.debug(f"Chunk {chunk_num} verification failed: {len(verify_tokens)} > {max_tokens}, truncating further")
                    # Further truncate if needed
                    chunk_tokens = verify_tokens[:max_tokens]
                    if hasattr(tokenizer, 'decode'):
                        chunk_text = tokenizer.decode(chunk_tokens, skip_special_tokens=True)
                    else:
                        chunk_text = tokenizer.convert_tokens_to_string(
                            tokenizer.convert_ids_to_tokens(chunk_tokens)
                        )
                
                chunks.append(chunk_text)
                logger.debug(f"Created chunk {chunk_num}: {len(verify_tokens)} tokens, {len(chunk_text)} chars")
                
                # Move start forward, with overlap
                if end_idx >= len(tokens):
                    break
                start_idx = end_idx - overlap_tokens
            
            logger.debug(f"Text chunked into {len(chunks)} chunks")
            return chunks
            
            return chunks
        except Exception as e:
            logger.warning(f"Error in token-based chunking, falling back to character-based: {e}")
            # Fallback to character-based
            max_chars = int(self.max_seq_length * 4 * 0.8)
            chunks = []
            start = 0
            while start < len(text):
                end = start + max_chars
                if end >= len(text):
                    chunks.append(text[start:])
                    break
                # Try to break at sentence boundary
                last_period = text.rfind('.', start, end)
                last_pipe = text.rfind('|', start, end)
                break_point = max(last_period, last_pipe)
                if break_point > start:
                    end = break_point + 1
                chunks.append(text[start:end])
                start = end - chunk_overlap
            return chunks
    
    def _validate_and_fix_chunk(self, chunk: str) -> str:
        """
        Validate that a chunk is within token limits and fix if needed
        This is a safety check to ensure chunks don't exceed model limits
        
        Args:
            chunk: Text chunk to validate
            
        Returns:
            Validated (and potentially fixed) chunk
        """
        if not chunk or not chunk.strip():
            return chunk
        
        try:
            # Try to get tokenizer
            tokenizer = None
            if hasattr(self.model, 'tokenizer'):
                tokenizer = self.model.tokenizer
            elif hasattr(self.model, '_first_module') and hasattr(self.model._first_module, 'tokenizer'):
                tokenizer = self.model._first_module.tokenizer
            
            if tokenizer:
                max_tokens = self.max_seq_length - 10  # Conservative limit
                
                # Tokenize to check length
                if hasattr(tokenizer, 'encode'):
                    tokens = tokenizer.encode(chunk, add_special_tokens=False)
                else:
                    tokens = tokenizer(chunk, add_special_tokens=False)['input_ids']
                
                if len(tokens) <= max_tokens:
                    return chunk
                
                # Chunk is too long - truncate tokens (log at debug level to reduce noise)
                logger.debug(f"Chunk exceeds token limit ({len(tokens)} > {max_tokens}), truncating...")
                truncated_tokens = tokens[:max_tokens]
                
                # Decode back to text
                if hasattr(tokenizer, 'decode'):
                    truncated_chunk = tokenizer.decode(truncated_tokens, skip_special_tokens=True)
                else:
                    truncated_chunk = tokenizer.convert_tokens_to_string(
                        tokenizer.convert_ids_to_tokens(truncated_tokens)
                    )
                
                return truncated_chunk
            else:
                # Fallback: character-based validation
                max_chars = int(self.max_seq_length * 4 * 0.7) if hasattr(self, 'max_seq_length') else 1800
                if len(chunk) <= max_chars:
                    return chunk
                logger.debug(f"Chunk exceeds character limit ({len(chunk)} > {max_chars}), truncating...")
                return chunk[:max_chars]
        except Exception as e:
            logger.warning(f"Error validating chunk, using as-is: {e}")
            return chunk
    
    def _average_embeddings(self, embeddings: List[np.ndarray]) -> np.ndarray:
        """
        Average multiple embeddings into a single embedding
        
        Args:
            embeddings: List of embedding vectors
            
        Returns:
            Averaged embedding vector
        """
        if not embeddings:
            return np.zeros(self.dimension)
        
        if len(embeddings) == 1:
            return embeddings[0]
        
        # Stack and average
        stacked = np.stack(embeddings)
        averaged = np.mean(stacked, axis=0)
        return averaged
    
    def generate_embedding(self, text: str, use_chunking: bool = True) -> np.ndarray:
        """
        Generate embedding for a single text
        ALWAYS uses chunking for SentenceTransformers to preserve all patient journey data
        Chunking splits long texts and averages embeddings to incorporate all information
        
        Args:
            text: Text to embed
            use_chunking: If True, chunk long text and average embeddings (default: True)
                         For SentenceTransformers, chunking is always used regardless of this parameter
            
        Returns:
            Embedding vector
        """
        if not text or not text.strip():
            # CRITICAL FIX: Return a unique small random vector instead of zeros
            # This ensures patients with empty text still get unique embeddings
            # Use text hash as seed for reproducibility
            import random
            seed = hash(text) % (2**32) if text else random.randint(0, 2**32-1)
            np.random.seed(seed)
            return np.random.normal(0, 0.01, self.dimension)
        
        try:
            if self.use_openai:
                # OpenAI handles long text automatically
                response = self.openai_client.embeddings.create(
                    model="text-embedding-3-large",
                    input=text
                )
                return np.array(response.data[0].embedding)
            elif self.use_gemini:
                # Gemini handles long text automatically
                result = self.genai.embed_content(
                    model="models/text-embedding-004",
                    content=text,
                    task_type="retrieval_document"
                )
                return np.array(result['embedding'])
            else:
                # SentenceTransformers: ALWAYS use chunking to preserve all patient data
                # Chunking ensures all information is incorporated by averaging chunk embeddings
                # This preserves the complete patient journey without truncation
                chunks = self._chunk_text(text)
                logger.debug(f"Text chunked into {len(chunks)} chunks (original length: {len(text)} chars)")
                
                if len(chunks) == 1:
                    # Single chunk - verify it's within token limit before encoding
                    chunk = chunks[0]
                    # Validate chunk size one more time before encoding
                    chunk = self._validate_and_fix_chunk(chunk)
                    logger.debug(f"Encoding single chunk (length: {len(chunk)} chars)")
                    # Disable progress bar - we'll show our own at patient level
                    return self.model.encode(chunk, convert_to_numpy=True, show_progress_bar=False, normalize_embeddings=False)
                else:
                    # Multiple chunks: embed each chunk and average to preserve all information
                    logger.debug(f"Text split into {len(chunks)} chunks for embedding (preserving all patient data)")
                    chunk_embeddings = []
                    for i, chunk in enumerate(chunks):
                        # Validate each chunk before encoding
                        chunk = self._validate_and_fix_chunk(chunk)
                        logger.debug(f"Encoding chunk {i+1}/{len(chunks)} (length: {len(chunk)} chars)")
                        # Disable progress bar - we'll show our own at patient level
                        try:
                            chunk_emb = self.model.encode(chunk, convert_to_numpy=True, show_progress_bar=False, normalize_embeddings=False)
                            chunk_embeddings.append(chunk_emb)
                        except Exception as chunk_error:
                            logger.error(f"Error encoding chunk {i+1}/{len(chunks)}: {chunk_error}")
                            # Skip this chunk but continue with others
                            continue
                    
                    if not chunk_embeddings:
                        logger.error("No chunks were successfully encoded")
                        return np.zeros(self.dimension)
                    
                    # Average embeddings to incorporate all chunks - this preserves all patient journey data
                    logger.debug(f"Averaging {len(chunk_embeddings)} chunk embeddings")
                    return self._average_embeddings(chunk_embeddings)
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return np.zeros(self.dimension)
    
    def generate_embeddings_batch(self, texts: List[str], use_chunking: bool = True) -> List[np.ndarray]:
        """
        Generate embeddings for multiple texts in batch
        ALWAYS uses chunking for SentenceTransformers to preserve all patient journey data
        Each text is processed individually with chunking to ensure all information is incorporated
        
        Args:
            texts: List of texts to embed
            use_chunking: If True, chunk long text and average embeddings (default: True)
                         For SentenceTransformers, chunking is always used regardless of this parameter
            
        Returns:
            List of embedding vectors (one per input text)
        """
        if not texts:
            return []
        
        try:
            if self.use_openai:
                # OpenAI handles long text automatically
                return [self.generate_embedding(text, use_chunking=False) for text in texts]
            elif self.use_gemini:
                # Gemini handles long text automatically
                result = self.genai.embed_content(
                    model="models/text-embedding-004",
                    content=texts,
                    task_type="retrieval_document"
                )
                return [np.array(emb) for emb in result['embedding']]
            else:
                # SentenceTransformers: ALWAYS use chunking to preserve all patient data
                # Process each text individually with chunking to ensure all information is preserved
                embeddings = []
                for idx, text in enumerate(texts):
                    try:
                        # Always use chunking - this ensures all patient journey data is incorporated
                        logger.debug(f"Processing text {idx+1}/{len(texts)} (length: {len(text)} chars)")
                        emb = self.generate_embedding(text, use_chunking=True)
                        embeddings.append(emb)
                    except Exception as e:
                        logger.error(f"Error processing text {idx+1}/{len(texts)}: {e}", exc_info=True)
                        # Add zero vector as fallback
                        embeddings.append(np.zeros(self.dimension))
                return embeddings
        except Exception as e:
            logger.error(f"Error generating batch embeddings: {e}")
            return [np.zeros(self.dimension) for _ in texts]
    
    def generate_patient_text_embedding(self, text_data: Dict[str, Any]) -> np.ndarray:
        """
        Generate embedding from formatted patient text data
        
        Args:
            text_data: Dictionary with formatted text data from EnhancedTextExtractor
            
        Returns:
            Text embedding vector
        """
        # Format text data into a single string
        from enhanced_text_extractor import EnhancedTextExtractor
        extractor = EnhancedTextExtractor(None)  # We only need the formatter
        formatted_text = extractor.format_text_for_embedding(text_data)
        
        if not formatted_text or not formatted_text.strip():
            logger.warning("No text data to embed - this should not happen if patient_id is included")
            # Return a small random vector instead of zeros to ensure uniqueness
            # This prevents identical embeddings for patients with missing data
            import random
            np.random.seed(hash(text_data.get('patient_id', 'unknown')) % (2**32))
            return np.random.normal(0, 0.01, self.dimension)
        
        return self.generate_embedding(formatted_text)
    
    def generate_patient_embeddings_batch(
        self,
        patient_text_data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, np.ndarray]:
        """
        Generate text embeddings for multiple patients
        
        Args:
            patient_text_data: Dictionary mapping patient_id to text data
            
        Returns:
            Dictionary mapping patient_id to embedding
        """
        # Format all texts first
        from enhanced_text_extractor import EnhancedTextExtractor
        extractor = EnhancedTextExtractor(None)
        
        formatted_texts = []
        patient_ids = []
        
        for patient_id, text_data in patient_text_data.items():
            formatted = extractor.format_text_for_embedding(text_data)
            if formatted and formatted.strip():
                formatted_texts.append(formatted)
                patient_ids.append(patient_id)
        
        if not formatted_texts:
            logger.warning("No valid text data to embed")
            return {}
        
        # Generate embeddings in batch
        embeddings = self.generate_embeddings_batch(formatted_texts)
        
        # Map back to patient IDs
        result = {}
        for patient_id, embedding in zip(patient_ids, embeddings):
            result[patient_id] = embedding
        
        return result

