"""
Simple test script for the Clinical GraphRAG Chatbot
"""
import logging
from query_processor import QueryProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_query(processor: QueryProcessor, query: str):
    """Test a single query"""
    print(f"\n{'='*80}")
    print(f"Query: {query}")
    print(f"{'='*80}\n")
    
    result = processor.process_query(query, top_k=10, generate_answer=True)
    
    print(f"Intent: {result.get('intent')}")
    print(f"Entities: {result.get('entities')}")
    print(f"\nAnswer:\n{result.get('answer', 'No answer generated')}")
    print(f"\n{'='*80}\n")

def main():
    """Main test function"""
    print("Initializing Clinical GraphRAG Chatbot...")
    
    try:
        processor = QueryProcessor()
        print("✓ Chatbot initialized successfully\n")
        
        # Test queries
        test_queries = [
            "Find patients similar to patient 100045",
            "What treatments worked best for cirrhosis?",
            "Summarize the hospital journey for patient 100023"
        ]
        
        for query in test_queries:
            try:
                test_query(processor, query)
            except Exception as e:
                print(f"Error testing query '{query}': {e}\n")
        
        processor.close()
        print("✓ Tests completed")
        
    except Exception as e:
        print(f"✗ Error initializing chatbot: {e}")
        print("\nPlease ensure:")
        print("1. Neo4j is running and accessible")
        print("2. Milvus is running and accessible")
        print("3. GEMINI_API_KEY is set in environment or .env file")

if __name__ == "__main__":
    main()

