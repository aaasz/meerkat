/*
 * dlinkedlist.h
 *
 * Implements a doubly linked list
 *
 *  Created on: Jun 2, 2018
 *      Author: aaasz
 */

#ifndef _MEERKATSTORE_DLINKEDLIST_H_
#define _MEERKATSTORE_DLINKEDLIST_H_

namespace meerkatstore {

template<typename T>
class DLinkedList
{
public:
	struct Node
	{
	    Node* prev;
	    T* key;
	    Node* next;

	    Node(Node* p, T* k, Node* n): prev{p}, key{k}, next{n} {}
	};
public:

    //default constructor creates a sentinel object
    DLinkedList() : count{ 0 }, head{ new Node(nullptr, nullptr, nullptr) }, tail{ head } { }
    ~DLinkedList();

    Node* begin() { return head; }
    Node* end() { return tail; }
    bool empty() { return head == tail; }
    uint32_t size() { return count; }
    T* front() { return head->key; }
    T* back() { return tail->prev->key; }

    void remove(Node*);
    Node* insert(Node* pos, T* key);
    Node* insert_sorted(T* key);
    Node* find(T* key);

private:

	uint32_t count;
	Node* head;
	Node* tail;
};

template<typename T>
DLinkedList<T>::~DLinkedList()
{
	Node* tmp;
	for (;head;head = tmp) {
		tmp = head->next;
		delete head;
	}
}

template<typename T>
inline typename DLinkedList<T>::Node* DLinkedList<T>::insert(Node* pos, T* key)
{
	Node* newNode;
    if (empty()) {
        newNode = new Node(nullptr, key, tail);
        head = newNode;
        tail->prev = newNode;
    } else {
        if (pos == head) {
            newNode = new Node(nullptr, key, head);
            head->prev = newNode;
            head = newNode;
        } else {
            if (pos == tail) {
                newNode = new Node(tail->prev, key, tail);
                tail->prev->next = newNode;
                tail->prev = newNode;
            } else {
                newNode = new Node(pos, key, pos->next);
                pos->next->prev = newNode;
                pos->next = newNode;
            }
        }
    }
    ++count;
    return newNode;
}

template<typename T>
inline void DLinkedList<T>::remove(Node* pos)
{
    if (empty()) {
        fprintf(stderr, "List is empty.");
    } else {
        if (pos == head) {
            Node* x = head;
            head = head->next;
            head->prev = nullptr;
            delete x;
        } else {
            Node* x = pos;
            pos->prev->next = pos->next;
            pos->next->prev = pos->prev;
            delete x;
        }
        --count;
    }
}

template<typename T>
inline typename DLinkedList<T>::Node* DLinkedList<T>::insert_sorted(T* key)
{
    if (empty()) {
        return insert(nullptr, key);
    } else {
        auto current = begin();

        // locate the node after which the new node
        // is to be inserted
        while (current != end() && *(current->key) < *key) {
        	current = current->next;
        }

        return insert(current, key);
    }
}

template<typename T>
inline typename DLinkedList<T>::Node* DLinkedList<T>::find(T* key)
{
	auto current = begin();

	while (current != end() && *(current->key) != *key) {
		current = current->next;
	}

	return current;
}


} // namespace meerkatstore

#endif /* _MEERKATSTORE_DLINKEDLIST_H_ */
