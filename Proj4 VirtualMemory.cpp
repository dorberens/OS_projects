#include "VirtualMemory.h"
#include "PhysicalMemory.h"

int initialize_frame_to_zeros(uint64_t frame_index)
{
    for(int i = 0; i<PAGE_SIZE; i++)
    {
        PMwrite(frame_index*PAGE_SIZE + i,0);
    }
    return 0;
}

void VMinitialize()
{
    initialize_frame_to_zeros(0);
}



/*
 * Find frame containing empty table- all rows are 0. Keeping the frame index, it's parent index and the row referencing it.
 */
void first_priority(uint64_t current_frame,uint64_t current_parent,word_t current_parent_row, uint64_t * all_zeros_frame,uint64_t *all_zeros_parent,int *all_zeros_row, uint64_t invalid_frame_index,int tree_depth)
{
    if(tree_depth == TABLES_DEPTH)
    {
        return;
    }
    // halting the recursion.
    bool all_rows_are_zeros = true;
    for(int i = 0; i<PAGE_SIZE;i++)
    {
        word_t read_value = 0;
        PMread(current_frame*PAGE_SIZE + i,&read_value);
        if(read_value!=0)
        {
            all_rows_are_zeros = false;
            first_priority(read_value, current_frame, i,all_zeros_frame,all_zeros_parent,all_zeros_row,invalid_frame_index,tree_depth+1);
        }
    }
    if(all_rows_are_zeros and current_frame!=invalid_frame_index and *all_zeros_frame==0)
    {
        *all_zeros_frame = current_frame;
        *all_zeros_parent = current_parent;
        *all_zeros_row = current_parent_row;
    }
}


void second_priority(int current_frame, int *max_frame, int tree_depth)
{
    if(*max_frame < current_frame)
    {
        *max_frame = current_frame;
    }
    if(tree_depth == TABLES_DEPTH){return;}    // halting the recursion.
    for(int i = 0; i<PAGE_SIZE;i++)
    {
        word_t read_value = 0;
        PMread(current_frame*PAGE_SIZE + i,&read_value);
        if(read_value != 0)
        {

            second_priority(read_value, max_frame,tree_depth+1);
        }
    }
}

int find_min(const int first_num, const int sec_num)
{
    if(first_num < sec_num){return first_num;}
    return sec_num;
}

int abs_value(const int number)
{
    if(number < 0){return -number;}
    return number;
}

void third_priority(uint64_t current_frame,uint64_t current_parent, int current_parent_row, int tree_depth, int p, uint64_t *evict_page,uint64_t *evict_frame,int *evict_value,uint64_t *evict_parent, int *evict_parent_row,  int page_index)
{
    if(tree_depth == TABLES_DEPTH)
    {
        int temp_value = find_min((int)NUM_PAGES - abs_value(page_index - p), abs_value(page_index - p));
        if(*evict_value < temp_value)
        {
            *evict_page = p;
            *evict_frame = current_frame;
            *evict_value = temp_value;
            *evict_parent = current_parent;
            *evict_parent_row = current_parent_row;
        }
        return;
    }
    for(int i = 0; i<PAGE_SIZE;i++)
    {
        word_t read_value = 0;
        PMread(current_frame*PAGE_SIZE + i,&read_value);
        if(read_value != 0)
        {
            int temp_p = p + i;
            if(tree_depth < TABLES_DEPTH-1)
            {
                temp_p = temp_p << OFFSET_WIDTH;
            }

            third_priority(read_value,current_frame, i, tree_depth+1, temp_p,evict_page,evict_frame,evict_value,evict_parent, evict_parent_row, page_index);
        }
    }
}

int apply_read_or_write_command(uint64_t virtualAddress, int command, word_t* read_value = nullptr, word_t write_value = 0)
{
    uint64_t pageIndex = virtualAddress / PAGE_SIZE;
    uint64_t pageOffset = virtualAddress % PAGE_SIZE;

    // creating table index array (p1, p2, p3...) *****************************
    uint64_t table_index_array[TABLES_DEPTH] = {};
    word_t shifter = PAGE_SIZE - 1;
    uint64_t partition_helper = pageIndex;
    for(int i = 0; i < TABLES_DEPTH; i++)
    {
        table_index_array[TABLES_DEPTH-1-i] = partition_helper & shifter;
        partition_helper = partition_helper >> OFFSET_WIDTH;
    }
    // ************************************************************************

    uint64_t address = 0;
    word_t new_address = 0;
    for(int i = 0;i<TABLES_DEPTH;i++)
    {
        PMread(address*PAGE_SIZE+table_index_array[i],&new_address);
        if(new_address == 0)
        {
            uint64_t all_zeros_frame = 0;
            uint64_t all_zeros_parent = 0;
            int all_zeros_row = 0;
            first_priority(0,0,0,&all_zeros_frame,&all_zeros_parent,&all_zeros_row,address,0);

            if(all_zeros_frame != 0)
            {
                PMwrite(all_zeros_parent*PAGE_SIZE + all_zeros_row, 0);
                if(i == TABLES_DEPTH-1)
                {
                    PMrestore(all_zeros_frame, pageIndex);
                }
//                    initialize_frame_to_zeros(all_zeros_frame);
                PMwrite(address*PAGE_SIZE+table_index_array[i], (word_t)all_zeros_frame);
                address = all_zeros_frame;
                continue;
            }
            int max_frame = 0;
            second_priority(0,&max_frame,0);
            if(max_frame + 1 < NUM_FRAMES)
            {
                if(i == TABLES_DEPTH-1)
                {
                    PMrestore(max_frame+1, pageIndex);
                }
                initialize_frame_to_zeros(max_frame+1);
                PMwrite(address*PAGE_SIZE+table_index_array[i], max_frame+1);
                address = max_frame+1;
                continue;
            }
            uint64_t evict_page = 0;
            uint64_t evict_frame = 0;
            int evict_value = 0;
            uint64_t evict_parent = 0;
            int evict_parent_row = 0;
            third_priority(0,0,0,0,0,&evict_page,&evict_frame,&evict_value,&evict_parent,&evict_parent_row, (word_t)pageIndex);
            PMevict(evict_frame, evict_page);
            PMwrite(evict_parent*PAGE_SIZE + evict_parent_row, 0);
            initialize_frame_to_zeros(evict_frame);
            if(i == TABLES_DEPTH-1)
            {
                PMrestore(evict_frame, pageIndex);
            }
            PMwrite(address*PAGE_SIZE+table_index_array[i], (word_t)evict_frame);
            address = evict_frame;
        }
        else
        {
            address = new_address;
        }
    }
    if(command == 0)
    {
        PMread(address*PAGE_SIZE+pageOffset, read_value);
    }
    if(command == 1)
    {
        PMwrite(address*PAGE_SIZE+pageOffset, write_value);
    }
    return 0;
}
/* Reads a word from the given virtual address
 * and puts its content in *value.
 *
 * returns 1 on success.
 * returns 0 on failure (if the address cannot be mapped to a physical
 * address for any reason)
 */
int VMread(uint64_t virtualAddress, word_t* value)
{
    if(virtualAddress >= VIRTUAL_MEMORY_SIZE){return 0;}
    // read command means 0!
    return !apply_read_or_write_command(virtualAddress, 0, value, 0);
}



/* Writes a word to the given virtual address.
 *
 * returns 1 on success.
 * returns 0 on failure (if the address cannot be mapped to a physical
 * address for any reason)
 */
int VMwrite(uint64_t virtualAddress, word_t value)
{
    if(virtualAddress >= VIRTUAL_MEMORY_SIZE){return 0;}
    // write command means 1!
    return !apply_read_or_write_command(virtualAddress, 1, nullptr, value);
}