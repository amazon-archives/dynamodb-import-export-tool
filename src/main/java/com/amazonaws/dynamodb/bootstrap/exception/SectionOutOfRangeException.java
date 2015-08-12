/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap.exception;

/**
 * Section is out of valid range Exception.
 */
public class SectionOutOfRangeException extends Exception {

    private static final long serialVersionUID = -4924652673622223172L;

    /**
     * Constructor calls superclass and nothing more.
     */
    public SectionOutOfRangeException(String s) {
        super(s);
    }
}